package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"gateway/internal/router"

	"go.uber.org/zap"
)

func main() {
	log, _ := zap.NewDevelopment()
	ctx, cancel := context.WithCancel(context.Background())

	srv, err := router.Setup(log, ctx)
	if err != nil {
		log.Fatal("Setup Server", zap.Error(err))
	}

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatal("Server start failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	gracefulShutdown(srv, log, cancel)
}

func gracefulShutdown(srv *router.Server, log *zap.Logger, closeFunc context.CancelFunc) {
	const op = "gateway.gracefulShutdown"

	log.Info("Shutting down server", zap.String("op", op))

	srv.Close()

	log.Info("Server shutdown successfully", zap.String("op", op))

	closeFunc()

	log.Info("Called close func", zap.String("op", op))
}
