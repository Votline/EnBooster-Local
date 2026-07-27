// Package router provides the connection to grpc and frontend
package router

import (
	"fmt"
	"net/http"

	"gateway/internal/delivery"

	"go.uber.org/zap"
)

// Server used for control and link http server with services
type Server struct {
	srv *http.Server
	log *zap.Logger
	wsh *delivery.WSHandler
}

func Setup(log *zap.Logger) (*Server, error) {
	const op = "router.Setup"
	wsh, err := delivery.NewWSH(log)
	if err != nil {
		return nil, fmt.Errorf("%s: create ws handler:%w", op, err)
	}
	mux := http.NewServeMux()
	wsh.RegisterRoutes(mux)

	return &Server{
		srv: &http.Server{
			Addr:    ":8080",
			Handler: mux,
		},
		log: log,
		wsh: wsh,
	}, nil
}

func (s *Server) Start() error {
	const op = "router.Start"
	if err := s.srv.ListenAndServe(); err != nil {
		return fmt.Errorf("%s: listen and serve: %w", op, err)
	}
	return nil
}
