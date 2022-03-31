package protos

import (
	"encoding/json"
)

type DelayMessage struct {
	Key     string `json:"-"`
	Ts      int64  `json:"ts"`
	Subject string `json:"subject"`
	Data    []byte `json:"data"`
}

func (me *DelayMessage) Encode() []byte {
	b, _ := json.Marshal(me)
	return b
}

func (me *DelayMessage) Decode(b []byte) error {
	err := json.Unmarshal(b, me)
	if me == nil {
		me = &DelayMessage{}
	}
	return err
}
