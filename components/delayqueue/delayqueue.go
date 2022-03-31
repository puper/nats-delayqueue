package delayqueue

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/google/orderedcode"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/puper/nats-delayqueue/components/delayqueue/protos"
	"go.etcd.io/bbolt"
)

var (
	UnknownTs  = int64(-1)
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
	if err := d.run(); err != nil {
		return nil, errors.Wrap(err, "delayqueue run error")
	}
	return d, nil
}

type DelayQueue struct {
	cfg      *Config
	natsConn *nats.Conn
	natsJs   nats.JetStreamContext
	bbolt    *bbolt.DB
	idgen    *snowflake.Node

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mutex  sync.RWMutex
	minTs  int64
}

func (me *DelayQueue) init() error {
	var err error
	me.idgen, err = snowflake.NewNode(0)
	if err != nil {
		return errors.Wrap(err, "idgen create error")
	}
	me.bbolt, err = bbolt.Open(me.cfg.Bbolt.Path, 0600, &bbolt.Options{})
	if err != nil {
		return errors.Wrap(err, "bbolt open error")
	}
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

func (me *DelayQueue) run() error {
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
					me.cfg.Logger.Warn("error", errors.Wrapf(err, "decode msg error"), "msgData", nmsg.Data)
				} else {
					log.Println("pulled: ", string(msg.Encode()))
					msgs = append(msgs, msg)
					if minTs == UnknownTs || msg.Ts < minTs {
						minTs = msg.Ts
					}
				}
			}
			err = me.bbolt.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(BucketName)
				for _, msg := range msgs {
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

/**
从数据库取完所有值，
*/
func (me *DelayQueue) writeloop() {
	tk := time.NewTicker(time.Millisecond * 1000)
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
		log.Printf("ts: %v, minTs: %v\n", ts, minTs)
		for {
			select {
			case <-me.ctx.Done():
				return
			default:
			}
			stop := false
			msgsKeys := [][]byte{}
			msgs := []*protos.DelayMessage{}
			ts = time.Now().Unix()
			minTs := UnknownTs
			err := me.bbolt.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(BucketName)
				c := bucket.Cursor()
				i := 0
				for k, v := c.First(); k != nil; k, v = c.Next() {
					msg := &protos.DelayMessage{}
					msgsKeys = append(msgsKeys, k)
					if err := msg.Decode(v); err != nil {
						// log
					} else {
						if msg.Subject == "" {
							// log
						} else {
							if msg.Ts <= ts {
								msg.Key = string(k)
								msgs = append(msgs, msg)
							} else {
								minTs = msg.Ts
								stop = true
								break
							}
						}
					}
					i++
					if i == 100 {
						return nil
					}
				}
				minTs = MaxTs
				stop = true
				return nil
			})
			err = nil
			if len(msgs) > 0 {
				for _, msg := range msgs {
					log.Println("pub: ", string(msg.Encode()))
					h := nats.Header{}
					h.Set("Nats-Msg-Id", msg.Key)
					nmsg := &nats.Msg{
						Subject: msg.Subject,
						Data:    msg.Data,
						Header:  h,
					}
					me.natsJs.PublishMsgAsync(nmsg)
				}
				tk := time.NewTicker(time.Second * 30)
				select {
				case <-me.natsJs.PublishAsyncComplete():
					tk.Stop()
				case <-tk.C:
					err = errors.New("publish msg timeout")
					tk.Stop()
				}
			}
			if err != nil {
				continue
			}
			err = me.bbolt.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(BucketName)
				for _, k := range msgsKeys {
					bucket.Delete(k)
				}
				return nil
			})
			if err != nil {
				time.Sleep(time.Second * 3)
				continue
			}
			if stop {

				if minTs != UnknownTs {
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
	b, _ := orderedcode.Append(nil, msg.Ts, me.idgen.Generate().Int64())
	return b
}
