package main

import (
	"gateway/internal/router"

	"go.uber.org/zap"
)

func main() {
	log, _ := zap.NewDevelopment()
	srv := router.Setup(log)
	if err := srv.Start(); err != nil {
		log.Fatal("Server start failed", zap.Error(err))
	}
}
