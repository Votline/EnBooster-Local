// Package learn tasks.go calls learn-service grpc methods
package learn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"enbstr/internal/services"

	pb "github.com/Votline/EnBooster/protos/generated-learn"
	"go.uber.org/zap"
)

// Task is a struct that represents a task
type Task struct {
	TaskData string `json:"task"`
	Level    string `json:"level"`
	Answer   string `json:"answer"`
	Position int32  `json:"position"`
	Theme    string `json:"theme"`
}

// NewTasks adds new tasks to the database
func (ls *LearnService) NewTasks(msg, reqTrace string) (int32, error) {
	const op = "learn.NewTasks"

	ls.log.Debug("New tasks request",
		zap.String("op", op),
		zap.Int("msg len", len(msg)),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	res, err := services.CallRPC(ls.cb, func() (*pb.NewTasksRes, error) {
		return ls.client.NewTasks(ctx, &pb.NewTasksReq{
			JsonData:     msg,
			RequestTrace: reqTrace,
		})
	})
	if err != nil {
		return 0, fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ls.log.Debug("New tasks response",
		zap.String("op", op),
		zap.Int32("inserted", res.Inserted),
		zap.String("reqTrace", reqTrace))

	return res.Inserted, nil
}

// GetTasks returns tasks from the database
func (ls *LearnService) GetTasks(level string, pos, limit int32, tasksList *[]Task, reqTrace string) error {
	const op = "learn.GetTasks"

	ls.log.Debug("Get tasks request",
		zap.String("op", op),
		zap.String("level", level),
		zap.Int32("pos", pos),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	tasks, err := services.CallRPC(ls.cb, func() (*pb.GetTasksRes, error) {
		return ls.client.GetTasks(ctx, &pb.GetTasksReq{
			Level:        level,
			Position:     pos,
			Limit:        limit,
			RequestTrace: reqTrace,
		})
	})
	if err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	tasksData := tasks.Data
	tasksBytes := unsafe.Slice(unsafe.StringData(tasksData), len(tasksData))

	if err := json.Unmarshal(tasksBytes, tasksList); err != nil {
		return fmt.Errorf("%s: unmarshal tasks: %w", op, err)
	}

	ls.log.Debug("Get tasks response",
		zap.String("op", op),
		zap.Int("tasks len", len(*tasksList)),
		zap.String("reqTrace", reqTrace))

	return nil
}

// DelTask deletes a task from the database
func (ls *LearnService) DelTask(msg, reqTrace string) error {
	const op = "learn.DelTask"

	ls.log.Debug("Delete task request",
		zap.String("op", op),
		zap.Int("msg len", len(msg)),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	msgBytes := unsafe.Slice(unsafe.StringData(msg), len(msg))
	level, pos, has := bytes.Cut(msgBytes, []byte(" "))
	if !has {
		return fmt.Errorf("%s: cut message: invalid message structure", op)
	}

	levelStr := unsafe.String(unsafe.SliceData(level), len(level))
	posStr := unsafe.String(unsafe.SliceData(pos), len(pos))
	posInt, err := strconv.Atoi(posStr)
	if err != nil {
		return fmt.Errorf("%s: atoi position: invalid position", op)
	}

	if _, err := services.CallRPC(ls.cb, func() (*pb.DelTaskRes, error) {
		return ls.client.DelTask(ctx, &pb.DelTaskReq{
			Level:        levelStr,
			Position:     int32(posInt),
			RequestTrace: reqTrace,
		})
	}); err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ls.log.Debug("Delete task successfully",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

func (ls *LearnService) VerifyAnswer(userAnswer, answer, reqTrace string) bool {
	const op = "learn.VerifyAnswer"

	correct := true
	answers := strings.Split(answer, ",")
	userAnswers := strings.Split(userAnswer, ",")

	if len(answers) != len(userAnswers) {
		return false
	}

	for i := range len(answers) {
		if answers[i] != userAnswers[i] {
			correct = false
			break
		}
	}

	return correct
}
