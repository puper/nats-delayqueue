# a delayqueue sidecar for nats' jetstream(IN DEVELOPING STAGE)

# usage
- create delay stream and consumer manually.
- update config/config.toml
- go run main.go serve --config=config/config.toml
- send msg by js

```
	msg := &protos.DelayMessage{
		Ts:      time.Now().Unix() + 10, // the timestamp you want to receive this msg
		Subject: "test", // the subject in jetstream you want to receive this msg
		Data:    []byte("123456"),
	}
	js.Publish("delayqueue", msg.Encode())
```