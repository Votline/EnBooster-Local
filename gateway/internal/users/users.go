// Package users user.go implement Service interface
// and make connect to users-service by gRPC
package users

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gateway/internal/statemanager"

	pb "github.com/Votline/EnBooster-Local/protos/generated-users"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// UsersService is a struct that implements Service interface
// and makes connect to users-service by gRPC
type UsersService struct {
	name       string
	langLevels map[string]struct{}
	notifyMsgs []string
	sm         *statemanager.StateManager
	ctxTimeout time.Duration
	log        *zap.Logger
	conn       *grpc.ClientConn
	client     pb.UsersServiceClient
}

// NewUS creates new UsersService instance
func NewUS(ctxTimeout time.Duration, sm *statemanager.StateManager, log *zap.Logger) (*UsersService, error) {
	const op = "users.NewUS"

	log.Info("Creating users service",
		zap.String("op", op))

	conn, err := grpc.NewClient(
		os.Getenv("USERS_HOST")+":"+os.Getenv("USERS_PORT"),
		grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create client: %w", op, err)
	}

	langLevels := make(map[string]struct{})
	langLevelsStr := os.Getenv("LANG_LEVELS")
	langLevelsSlc := strings.Split(langLevelsStr, " ")
	for _, lvl := range langLevelsSlc {
		if lvl == "" {
			continue
		}
		langLevels[lvl] = struct{}{}
	}

	notifyMsgs := strings.Split(os.Getenv("NOTIFICATION_MESSAGES"), ";")

	srv := &UsersService{
		name:       "users",
		langLevels: langLevels,
		notifyMsgs: notifyMsgs,
		sm:         sm,
		ctxTimeout: ctxTimeout,
		log:        log,
		conn:       conn,
		client:     pb.NewUsersServiceClient(conn),
	}

	return srv, nil
}

// Close closes the connection to the server
func (us *UsersService) Close() error {
	const op = "users.Close"

	errStr := ""
	if err := us.conn.Close(); err != nil {
		errStr += err.Error()
	}
	if errStr != "" {
		return fmt.Errorf("%s: %s", op, errStr)
	}

	return nil
}

// GetName returns the name of the service
func (us *UsersService) GetName() string {
	return us.name
}
