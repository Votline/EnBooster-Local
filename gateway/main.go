package main

import (
	"gateway/internal/router"

	"go.uber.org/zap"
)

func main() {
	log, _ := zap.NewDevelopment()
	srv, err := router.Setup(log)
	if err != nil {
		log.Fatal("Setup Server", zap.Error(err))
	}
	if err := srv.Start(); err != nil {
		log.Fatal("Server start failed", zap.Error(err))
	}
}
