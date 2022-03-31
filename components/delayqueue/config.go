package delayqueue

import "go.uber.org/zap"

type Config struct {
	Nats   *NatsConfig        `json:"nats"`
	Bbolt  *BboltConfig       `json:"bbolt"`
	Logger *zap.SugaredLogger `json:"-"`
}

type NatsConfig struct {
	Url          string `json:"url"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	StreamName   string `json:"streamName"`
	ConsumerName string `json:"consumerName"`
	Subject      string `json:"subject"`
}

type BboltConfig struct {
	Path string `json:"path"`
}
