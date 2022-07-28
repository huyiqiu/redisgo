package main

import (
	"fmt"
	"os"
	"redisgo/config"
	"redisgo/lib/logger"
	"redisgo/tcp"
)

const configfile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(filename string) bool {
	fi, err := os.Stat(filename)
	return err == nil && !fi.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{
		Path: "logs",
		Name: "redisgo",
		Ext: "log",
		TimeFormat: "2006-01-02",
	})

	if fileExists(configfile) {
		config.SetupConfig(configfile)
	} else {
		config.Properties = defaultProperties
	}

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		},
		tcp.MakeHandler(),
	)

	if err != nil {
		logger.Error(err)
	}
}