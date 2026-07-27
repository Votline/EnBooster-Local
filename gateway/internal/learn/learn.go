// Package learn learn.go implement Service interface
// and make connect to learn-service by gRPC
package learn

import (
	"fmt"
	"os"
	"time"

	"gateway/internal/statemanager"

	pb "github.com/Votline/EnBooster-Local/protos/generated-learn"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// LearnService is a struct that implements Service interface
// and makes connect to learn-service by gRPC
type LearnService struct {
	name       string
	ctxTimeout time.Duration
	log        *zap.Logger
	conn       *grpc.ClientConn
	client     pb.LearnServiceClient
	states     *statemanager.StateManager
}

// NewLS creates new LearnService instance
func NewLS(states *statemanager.StateManager, ctxTimeout time.Duration, log *zap.Logger) (*LearnService, error) {
	const op = "learn.NewLS"

	log.Info("Creating learn service",
		zap.String("op", op))

	conn, err := grpc.NewClient(
		os.Getenv("LEARN_HOST")+":"+os.Getenv("LEARN_PORT"),
		grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create client: %w", op, err)
	}

	return &LearnService{
		name:       "learn",
		ctxTimeout: ctxTimeout,
		log:        log,
		conn:       conn,
		states:     states,
		client:     pb.NewLearnServiceClient(conn),
	}, nil
}

// Close closes the connection to the server
func (ls *LearnService) Close() error {
	return ls.conn.Close()
}

// GetName returns the name of the service
func (ls *LearnService) GetName() string {
	return ls.name
}
