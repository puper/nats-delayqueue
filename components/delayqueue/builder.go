package delayqueue

import (
	"github.com/puper/ppgo/helpers"
	"github.com/puper/ppgo/v2/components/zaplog"
	"github.com/puper/ppgo/v2/engine"
)

func Builder(cfgKey string) func(*engine.Engine) (interface{}, error) {
	return func(e *engine.Engine) (interface{}, error) {
		cfg := e.GetConfig().Get(cfgKey)
		c := &Config{}
		if err := helpers.StructDecode(cfg, c, "json"); err != nil {
			return nil, err
		}
		c.Logger = e.Get("log").(*zaplog.Log).Get("")
		return New(
			c,
		)
	}
}
