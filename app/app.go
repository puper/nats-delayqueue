package app

import (
	zaplog "github.com/puper/ppgo/v2/components/zaplog"
	"github.com/puper/ppgo/v2/engine"
	"go.uber.org/zap"
)

var (
	app *engine.Engine
)

func Set(e *engine.Engine) {
	app = e
}

func Get() *engine.Engine {
	return app
}

func GetConfig() *engine.Config {
	return app.GetConfig()
}

func GetLog(name string) *zap.SugaredLogger {
	return app.Get("log").(*zaplog.Log).Get(name)
}
