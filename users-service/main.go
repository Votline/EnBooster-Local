// Package main provides users service grpc methods.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"unsafe"

	"users/internal/db"

	pb "github.com/Votline/EnBooster-Local/protos/generated-users"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// usersserver provides users service grpc methods.
type usersserver struct {
	db     *db.DB
	log    *zap.Logger
	reader *kafka.Reader
	pb.UnimplementedUsersServiceServer
}

// UserAnswer used to apply user answer from kafka
type UserAnswer struct {
	UUID      int64  `json:"uuid"`
	Correct   bool   `json:"correct"`
	Theme     string `json:"theme"`
	Counter   int    `json:"counter"`
	RequestID string `json:"request_id"`
}

func main() {
	log, _ := zap.NewDevelopment()
	defer log.Sync()

	lis, err := net.Listen("tcp", ":"+os.Getenv("USERS_PORT"))
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	pdb, err := db.NewDB(log)
	if err != nil {
		log.Fatal("failed to create db", zap.Error(err))
	}
	defer pdb.Close()

	s := usersserver{log: log, db: pdb}
	srv := grpc.NewServer()
	pb.RegisterUsersServiceServer(srv, &s)

	log.Debug("Users service successfully started")

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatal("failed to serve: ", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	gracefulShutdown(&s, srv)
}

func gracefulShutdown(s *usersserver, srv *grpc.Server) {
	const op = "usersserver.gracefulShutdown"

	s.log.Info("Shutting down server", zap.String("op", op))

	srv.Stop()
	s.log.Info("Server stopped", zap.String("op", op))

	if err := s.db.Close(); err != nil {
		s.log.Error("Failed to close database",
			zap.String("op", op),
			zap.Error(err))
	}
	s.log.Info("Database closed", zap.String("op", op))

	s.log.Info("Server shutdown successfully", zap.String("op", op))
}

// GetUser get user from database with uuid from request.
// Returns all user fields if user exists
func (s *usersserver) GetUser(ctx context.Context, req *pb.GetReq) (*pb.GetRes, error) {
	const op = "usersserver.GetUser"

	var uuid int64 = 1
	reqTrace := req.GetRequestTrace()

	s.log.Debug("GetUser request",
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	ud, err := s.db.GetUser(uuid, ctx, reqTrace)
	if err != nil {
		return nil, fmt.Errorf("%s: db get user: %w", op, err)
	}

	s.log.Debug("Successfully got user",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	jsonData, err := json.Marshal(ud)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal user: %w", op, err)
	}
	jsonStr := unsafe.String(unsafe.SliceData(jsonData), len(jsonData))

	return &pb.GetRes{
		Data: jsonStr,
	}, nil
}

// UpdSystemPrompt updates the system prompt of a user
func (s *usersserver) UpdSystemPrompt(ctx context.Context, req *pb.UpdSystemPromptReq) (*pb.UpdSystemPromptRes, error) {
	const op = "usersserver.UpdSystemPrompt"

	var uuid int64 = 1
	reqTrace := req.GetRequestTrace()
	sp := req.GetSystemPrompt()

	s.log.Debug("UpdSystemPromnt request",
		zap.Int64("uuid", uuid),
		zap.Int("system_prompt_len", len(sp)),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if err := s.db.UpdateSystemPrompt(ctx, uuid, sp, reqTrace); err != nil {
		return nil, fmt.Errorf("%s: db update system prompt: %w", op, err)
	}

	s.log.Debug("Successfully updated system prompt",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return &pb.UpdSystemPromptRes{}, nil
}

// UpdLangLevel updates user lang level
func (s *usersserver) UpdLangLevel(ctx context.Context, req *pb.UpdLangLevelReq) (*pb.UpdLangLevelRes, error) {
	const op = "usersserver.UpdLangLevel"

	var uuid int64 = 1
	level := req.GetLevel()
	reqTrace := req.GetRequestTrace()

	s.log.Debug("UpdLangLevel request",
		zap.Int64("uuid", uuid),
		zap.String("lang_level", level),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if err := s.db.UpdateLangLevel(ctx, uuid, level, reqTrace); err != nil {
		return nil, fmt.Errorf("%s: db update lang level: %w", op, err)
	}

	s.log.Debug("Successfully updated lang level",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return &pb.UpdLangLevelRes{}, nil
}

// UpdStreak updates user streak
func (s *usersserver) UpdStreak(ctx context.Context, req *pb.UpdStreakReq) (*pb.UpdStreakRes, error) {
	const op = "usersserver.UpdStreak"

	var uuid int64 = 1
	correct := req.GetCorrect()
	theme := req.GetTheme()
	counter := req.GetCounter()
	reqTrace := req.GetRequestTrace()

	s.log.Debug("UpdStreak request",
		zap.Bool("correct", correct),
		zap.String("theme", theme),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if err := s.db.UpdateStreak(uuid, ctx, reqTrace, correct, theme, counter); err != nil {
		return nil, fmt.Errorf("%s: db update streak: %w", op, err)
	}

	s.log.Debug("Successfully updated streak",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return &pb.UpdStreakRes{}, nil
}
