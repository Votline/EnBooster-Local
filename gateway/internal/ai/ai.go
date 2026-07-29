// Package ai ai.go implement Service interface
// and make connect to ai-service by gRPC
package ai

import (
	"fmt"
	"os"
	"time"

	stm "gateway/internal/statemanager"

	pb "github.com/Votline/EnBooster-Local/protos/generated-ai"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// AIService is a struct that implements Service interface
// and makes connect to ai-service by gRPC
type AIService struct {
	name       string
	ctxTimeout time.Duration
	sm         *stm.StateManager
	log        *zap.Logger
	conn       *grpc.ClientConn
	client     pb.AIServiceClient
}

// NewAIS creates new AIService instance
func NewAIS(ctxTimeout time.Duration, sm *stm.StateManager, log *zap.Logger) (*AIService, error) {
	const op = "ai.NewAIS"

	log.Info("Creating ai service",
		zap.String("op", op))

	conn, err := grpc.NewClient(
		os.Getenv("AISRV_HOST")+":"+os.Getenv("AISRV_PORT"),
		grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create client: %w", op, err)
	}

	srv := &AIService{
		name:       "ai",
		ctxTimeout: ctxTimeout,
		sm:         sm,
		log:        log,
		conn:       conn,
		client:     pb.NewAIServiceClient(conn),
	}

	return srv, nil
}

// Close closes the connection to the server
func (ai *AIService) Close() error {
	return ai.conn.Close()
}

// GetName returns the name of the service
func (ai *AIService) GetName() string {
	return ai.name
}
