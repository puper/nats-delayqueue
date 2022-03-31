package bootstrap

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/puper/nats-delayqueue/app"
	"github.com/puper/nats-delayqueue/components/delayqueue"
	"github.com/puper/ppgo/v2/components/zaplog"
	"github.com/puper/ppgo/v2/engine"
	"github.com/spf13/viper"
)

func Bootstrap(cfgFile string) error {
	log.Printf("using config file: %v\n", cfgFile)
	conf := viper.New()
	conf.SetConfigFile(cfgFile)
	if err := conf.ReadInConfig(); err != nil {
		return err
	}
	conf.AutomaticEnv()
	e := engine.New(conf)
	app.Set(e)
	e.Register("log", zaplog.Builder("log"))
	e.Register("delayqueue", delayqueue.Builder("delayqueue"), "log")
	err := e.Build()
	if err != nil {
		return err
	}
	defer e.Close()
	stop := make(chan struct{})
	go func() {
		sChan := make(chan os.Signal)
		for {
			signal.Notify(sChan, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
			sig := <-sChan
			switch sig {
			case os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				stop <- struct{}{}
			}

		}
	}()
	<-stop
	return nil
}
