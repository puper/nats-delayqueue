package delayqueue

import (
	"context"
	"encoding/base64"
	"math"
	"sync"
	"time"

	"github.com/google/orderedcode"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/puper/nats-delayqueue/components/delayqueue/protos"
	"go.etcd.io/bbolt"
)

var (
	// unable to determine the min ts
	UnknownTs = int64(-1)
	// indecated no pending msg now
	MaxTs      = int64(math.MaxInt64)
	BucketName = []byte("delayqueue")
)

func New(cfg *Config) (*DelayQueue, error) {
	d := &DelayQueue{
		cfg:   cfg,
		minTs: UnknownTs,
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	if err := d.init(); err != nil {
		return nil, errors.Wrap(err, "delayqueue init error")
	}
	if err := d.start(); err != nil {
		return nil, errors.Wrap(err, "delayqueue run error")
	}
	d.minTs = UnknownTs
	return d, nil
}

type DelayQueue struct {
	cfg      *Config
	natsConn *nats.Conn
	natsJs   nats.JetStreamContext
	bbolt    *bbolt.DB
	//idgen    *snowflake.Node

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mutex  sync.RWMutex
	minTs  int64
}

func (me *DelayQueue) init() error {
	var err error
	/**
	me.idgen, err = snowflake.NewNode(0)
	if err != nil {
		return errors.Wrap(err, "idgen create error")
	}
	*/
	me.bbolt, err = bbolt.Open(me.cfg.Bbolt.Path, 0600, &bbolt.Options{})
	if err != nil {
		return errors.Wrap(err, "bbolt open error")
	}
	// ensure bucket exists
	me.bbolt.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(BucketName)
		return err
	})
	c, err := nats.Connect(
		me.cfg.Nats.Url,
		nats.UserInfo(me.cfg.Nats.Username, me.cfg.Nats.Password),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second*3),
	)
	if err != nil {
		return errors.Wrap(err, "nats connect error")
	}
	js, err := c.JetStream()
	if err != nil {
		return errors.Wrap(err, "nats init jetstream error")
	}
	_, err = js.StreamInfo(me.cfg.Nats.StreamName)
	if err != nil {
		return errors.Wrapf(err, "nats get streamInfo `%v` error", me.cfg.Nats.StreamName)
	}
	_, err = js.ConsumerInfo(me.cfg.Nats.StreamName, me.cfg.Nats.ConsumerName)
	if err != nil {
		return errors.Wrapf(err, "nats get consumerInfo `%v` error", me.cfg.Nats.ConsumerName)
	}
	me.natsConn = c
	me.natsJs = js
	return nil
}

func (me *DelayQueue) start() error {
	me.wg.Add(2)
	go func() {
		me.readloop()
		me.wg.Done()
	}()
	go func() {
		me.writeloop()
		me.wg.Done()
	}()
	return nil
}

func (me *DelayQueue) Close() error {
	me.cancel()
	waitc := make(chan bool, 1)
	go func() {
		me.wg.Wait()
		waitc <- true
	}()
	select {
	case <-time.After(time.Second * 30):
		me.natsConn.Close()
		return errors.Errorf("close timeout")
	case <-waitc:
		me.natsConn.Close()
		return nil
	}
}

func (me *DelayQueue) readloop() {
	for {
		sub, err := me.natsJs.PullSubscribe(
			me.cfg.Nats.Subject,
			me.cfg.Nats.ConsumerName,
			nats.Bind(me.cfg.Nats.StreamName, me.cfg.Nats.ConsumerName),
		)
		if err != nil {
			me.cfg.Logger.Warn("error", errors.Wrapf(err, "PullSubscribe error"))
			select {
			case <-me.ctx.Done():
				return
			default:
			}
			time.Sleep(time.Second * 3)
			continue
		}
		defer sub.Drain()
		defer sub.Unsubscribe()
		for {
			select {
			case <-me.ctx.Done():
				return
			default:
			}
			nmsgs, err := sub.Fetch(100, nats.MaxWait(time.Second))
			if err != nil {
				if err != nats.ErrTimeout {
					//@todo there may be no messages error, not real error.
					me.cfg.Logger.Warn("error", errors.Wrapf(err, "Fetch msg error"))
					time.Sleep(time.Second * 3)
					continue
				}
			}
			msgs := make([]*protos.DelayMessage, 0, len(nmsgs))
			minTs := UnknownTs
			for _, nmsg := range nmsgs {
				msg := &protos.DelayMessage{}
				if err := msg.Decode(nmsg.Data); err != nil {
					me.cfg.Logger.Warn("error", errors.Wrapf(err, "msg.Decode.Error"), "msgData", nmsg.Data)
				} else {
					me.cfg.Logger.Debugw("msg.Pulled", "msgData", msg)
					msgs = append(msgs, msg)
					if minTs == UnknownTs || msg.Ts < minTs {
						minTs = msg.Ts
					}
				}
			}
			err = me.bbolt.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(BucketName)
				for _, msg := range msgs {
					//msgKey := me.genMsgKey(msg)
					//log.Println("msgKey, ", string(msgKey))
					err := bucket.Put(me.genMsgKey(msg), msg.Encode())
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				me.cfg.Logger.Warn("error", errors.Wrapf(err, "bboltdb save msg error"))
				for _, nmsg := range nmsgs {
					nmsg.Nak()
				}
			} else {
				for _, nmsg := range nmsgs {
					nmsg.Ack()
				}
				me.mutex.Lock()
				if minTs != UnknownTs && minTs < me.minTs {
					me.minTs = minTs
				}
				me.mutex.Unlock()
			}
		}
	}
}

func (me *DelayQueue) writeloop() {
	tk := time.NewTicker(time.Millisecond * 300)
	for range tk.C {
		select {
		case <-me.ctx.Done():
			return
		default:
		}
		me.mutex.RLock()
		minTs := me.minTs
		me.mutex.RUnlock()
		ts := time.Now().Unix()
		if minTs != UnknownTs && ts < minTs {
			continue
		}
		for {
			select {
			case <-me.ctx.Done():
				return
			default:
			}
			// break loop when time not reached or no more msg in bbolt
			breakLoop := false
			msgKeys := [][]byte{}
			msgs := []*protos.DelayMessage{}
			ts = time.Now().Unix()
			minTs := UnknownTs
			err := me.bbolt.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(BucketName)
				c := bucket.Cursor()
				i := 0
				for k, v := c.First(); k != nil; k, v = c.Next() {
					msg := &protos.DelayMessage{}
					if err := msg.Decode(v); err != nil {
						msgKeys = append(msgKeys, k)
						me.cfg.Logger.Warnf("msg.Invalid.Error", "error", err, "msgData", v)
					} else {
						if msg.Subject == "" {
							msgKeys = append(msgKeys, k)
							me.cfg.Logger.Warnf("msg.Invalid.Error: subject empty", "error", err, "msgData", msg)
						} else {
							if msg.Ts <= ts {
								msgKeys = append(msgKeys, k)
								msg.Key = base64.StdEncoding.EncodeToString(k)
								msgs = append(msgs, msg)
							} else {
								minTs = msg.Ts
								breakLoop = true
								return nil
							}
						}
					}
					i++
					if i == 100 {
						return nil
					}
				}
				minTs = MaxTs
				breakLoop = true
				return nil
			})
			if err != nil {
				// err but not care
				me.cfg.Logger.Info("bbolt.View.Error", "error", err)
			}
			err = nil
			errMsgKeys := map[string]bool{}
			if len(msgs) > 0 {
				//acks := map[string]nats.PubAckFuture{}
				for _, msg := range msgs {
					me.cfg.Logger.Debugw("msgPubed", "msgData", msg)
					h := nats.Header{}
					h.Set("Nats-Msg-Id", msg.Key)
					nmsg := &nats.Msg{
						Subject: msg.Subject,
						Data:    msg.Data,
						Header:  h,
					}
					//ack, err := me.natsJs.PublishMsgAsync(nmsg)
					_, err := me.natsJs.PublishMsgAsync(nmsg)
					if err != nil {
						errMsgKeys[msg.Key] = true
						me.cfg.Logger.Warnf("msg.Pub.Error", "error", err)
					} else {
						//acks[msg.Key] = ack
					}
				}
				/**
				tm := time.NewTimer(time.Second * 10)
				for msgKey, ack := range acks {
					if tm.Stop() {
						select {
						case err := <-ack.Err():
							log.Println("ackErr:", err)
							errMsgKeys[msgKey] = true
						case <-ack.Ok():
						default:
							errMsgKeys[msgKey] = true
						}
						continue
					}
					select {
					case err := <-ack.Err():
						log.Println("ackErr:", err)
						errMsgKeys[msgKey] = true
					case <-ack.Ok():
						log.Println("accOk")
					case <-tm.C:
						errMsgKeys[msgKey] = true
					}
				}
				if !tm.Stop() {
					tm.Stop()
				}
				tm.Stop()
				*/
			}
			err = me.bbolt.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(BucketName)
				for _, k := range msgKeys {
					if errMsgKeys[base64.StdEncoding.EncodeToString(k)] {
						continue
					}
					err := bucket.Delete(k)
					if err != nil {
						return errors.Wrapf(err, "bbolt.Delete.Error")
					}
				}
				return nil
			})
			if err != nil {
				me.cfg.Logger.Warnf("bbolt.Update.Error", "error", err)
				time.Sleep(time.Second * 3)
				continue
			}
			if breakLoop {
				if minTs != UnknownTs && len(errMsgKeys) == 0 {
					me.mutex.Lock()
					if minTs < me.minTs || me.minTs == UnknownTs {
						me.minTs = minTs
					} else {
						me.minTs = UnknownTs
					}
					me.mutex.Unlock()
				}
				break
			}
		}
	}
}

func (me *DelayQueue) genMsgKey(msg *protos.DelayMessage) []byte {
	b, _ := orderedcode.Append(nil, msg.Ts, time.Now().UnixNano())
	return b
}
