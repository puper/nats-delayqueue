# a delayqueue sidecar for nats' jetstream

# usage
- create delay stream and consumer manually
```
	js.AddStream(&nats.StreamConfig{
		Name:     "delayqueue",
		Subjects: []string{"delayqueue"},
	})
	c, err := js.AddConsumer("delayqueue", &nats.ConsumerConfig{
		Durable:    "delayqueue",
		MaxDeliver: -1,
		AckPolicy:  nats.AckExplicitPolicy,
	})
```
- update config/config.toml
- go run main.go serve --config=config/config.toml
- send msg as you do before but sent to the delayqueue

```
	msg := &protos.DelayMessage{
		Ts:      time.Now().Unix() + 10, // the timestamp you want to receive this msg
		Subject: "test", // the subject in jetstream you want to receive this msg
		Data:    []byte("123456"),
	}
	js.Publish("delayqueue", msg.Encode())
```