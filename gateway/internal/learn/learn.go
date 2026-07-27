// Package learn learn.go implement Service interface
// and make connect to learn-service by gRPC
package learn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"enbstr/internal/cbreaker"
	"enbstr/internal/statemanager"

	pb "github.com/Votline/EnBooster/protos/generated-learn"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	tele "gopkg.in/telebot.v3"
)

// LearnService is a struct that implements Service interface
// and makes connect to learn-service by gRPC
type LearnService struct {
	name       string
	ctxTimeout time.Duration
	log        *zap.Logger
	conn       *grpc.ClientConn
	cb         *gobreaker.CircuitBreaker[any]
	client     pb.LearnServiceClient
	states     *statemanager.StateManager
}

// NewLS creates new LearnService instance
func NewLS(states *statemanager.StateManager, ctxTimeout time.Duration, log *zap.Logger) (*LearnService, error) {
	const op = "learn.NewLS"

	log.Info("Creating learn service",
		zap.String("op", op))

	caCert, err := os.ReadFile("ssl/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("%s: get certs: %w", op, err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)

	config := &tls.Config{
		RootCAs:    certPool,
		ServerName: os.Getenv("TLS_SERVER_NAME"),
	}

	conn, err := grpc.NewClient(
		os.Getenv("LEARN_HOST")+":"+os.Getenv("LEARN_PORT"),
		grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create client: %w", op, err)
	}

	return &LearnService{
		name:       "learn",
		ctxTimeout: ctxTimeout,
		log:        log,
		conn:       conn,
		states:     states,
		cb:         cbreaker.NewCB("learn", log),
		client:     pb.NewLearnServiceClient(conn),
	}, nil
}

// HandleRoutes handle user messages which intended for learn-service
func (ls *LearnService) HandleRoutes(msg string, c tele.Context) error {
	const op = "learn.HandleRoutes"

	return nil
}

// Close closes the connection to the server
func (ls *LearnService) Close() error {
	return ls.conn.Close()
}

// GetName returns the name of the service
func (ls *LearnService) GetName() string {
	return ls.name
}
