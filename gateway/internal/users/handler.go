// Package users handler.go contains methods for
// call users service grpc methods
package users

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"

	stm "gateway/internal/statemanager"

	pb "github.com/Votline/EnBooster-Local/protos/generated-users"
	"go.uber.org/zap"
)

// UserData is a struct that represents user data
type UserData struct {
	UUID          int64  `json:"uuid"`
	BestTheme     string `json:"best_theme"`
	BestThemeCnt  int32  `json:"best_theme_counter"`
	WorstTheme    string `json:"worst_theme"`
	WorstThemeCnt int32  `json:"worst_theme_counter"`
	Level         string `json:"level"`
	TaskID        int32  `json:"task_id"`
	Streak        int32  `json:"streak"`
	SystemPrompt  string `json:"system_prompt"`
}

// UserAnswer used to push user answer to kafka
type UserAnswer struct {
	UUID      int64  `json:"uuid"`
	Correct   bool   `json:"correct"`
	Counter   int    `json:"counter"`
	Theme     string `json:"theme"`
	RequestID string `json:"request_id"`
}

// GetData returns user data
func (us *UsersService) GetData(reqTrace string) (UserData, error) {
	const op = "users.GetData"

	us.log.Debug("Get user data request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), us.ctxTimeout)
	defer cancel()

	res, err := us.client.GetUser(ctx, &pb.GetReq{
		RequestTrace: reqTrace,
	})
	if err != nil {
		return UserData{}, fmt.Errorf("%s: rpc call: %w", op, err)
	}

	dataBytes := unsafe.Slice(unsafe.StringData(res.Data), len(res.Data))

	var userData UserData
	if err := json.Unmarshal(dataBytes, &userData); err != nil {
		return UserData{}, fmt.Errorf("%s: unmarshal: %w", op, err)
	}

	us.log.Debug("Get user data successfully",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return userData, nil
}

// UpdSystemPrompt updates user system prompt
func (us *UsersService) UpdSystemPrompt(uuid int64, sp, reqTrace string) error {
	const op = "users.UpdSystemPrompt"

	us.log.Debug("Update system prompt request",
		zap.String("op", op),
		zap.Int64("uuid", uuid),
		zap.Int("system_prompt_len", len(sp)),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), us.ctxTimeout)
	defer cancel()

	if _, err := us.client.UpdSystemPrompt(ctx, &pb.UpdSystemPromptReq{
		SystemPrompt: sp,
		RequestTrace: reqTrace,
	}); err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	us.log.Debug("Update system prompt successfully",
		zap.String("op", op),
		zap.Int64("uuid", uuid),
		zap.String("reqTrace", reqTrace))

	return nil
}

// UpdLangLevel updates the language level of the user
func (us *UsersService) UpdLangLevel(uuid int64, level string, reqTrace string) error {
	const op = "users.UpdLangLevel"

	us.log.Debug("Udp language level request",
		zap.String("op", op),
		zap.Int64("uuid", uuid),
		zap.String("level", level),
		zap.String("reqTrace", reqTrace))

	if _, ok := us.langLevels[level]; !ok {
		return fmt.Errorf("%s: invalid language level: %s", op, level)
	}

	ctx, cancel := context.WithTimeout(context.Background(), us.ctxTimeout)
	defer cancel()

	if _, err := us.client.UpdLangLevel(ctx, &pb.UpdLangLevelReq{
		Level:        level,
		RequestTrace: reqTrace,
	}); err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	us.log.Debug("Set language level successfully",
		zap.String("op", op),
		zap.Int64("uuid", uuid),
		zap.String("level", level),
		zap.String("reqTrace", reqTrace))

	return nil
}

// UpdateUserTaskCtx get user context, update it previous counter
// and set new state
func (us *UsersService) UpdateUserTaskCtx(uctx stm.UserContext, theme, answer, reqTrace string, add int) error {
	const op = "users.UpdateUserTaskCtx"

	us.log.Debug("Update user task ctx request",
		zap.String("reqTrace", reqTrace),
		zap.String("op", op))

	var taskSes stm.TaskSession
	uctxData := unsafe.Slice(unsafe.StringData(uctx.JSONData), len(uctx.JSONData))
	if uctx.JSONData != "" {
		if err := json.Unmarshal(uctxData, &taskSes); err != nil {
			return fmt.Errorf("%s: unmarshal: %w", op, err)
		}
	}

	curSes := stm.TaskSession{
		CurrentTheme: theme,
		Counter:      taskSes.Counter + add,
		Answer:       answer,
	}

	jsonData, err := json.Marshal(curSes)
	if err != nil {
		return fmt.Errorf("%s: marshal json: %w", op, err)
	}

	if err := us.sm.UpdateUserDataCtx(jsonData); err != nil {
		return fmt.Errorf("%s: update data: %w", op, err)
	}

	us.log.Debug("Update user task ctx successfully",
		zap.String("reqTrace", reqTrace),
		zap.String("op", op))

	return nil
}

// UpdateUserShiritoriCtx updates user shiritori context
// append new word to the used words
// check if the word is repeated - return true if it is
func (us *UsersService) UpdateUserShiritoriCtx(
	shiritoriSes *stm.ShiritoriSession,
	userWord, userLastLetter, botWord, botLastLetter string,
	offsetID int, state int8,
) (isRepeat bool, notMatch bool, err error) {
	const op = "users.UpdateUserShiritoriCtx"

	us.log.Debug("Is word repeated request",
		zap.String("op", op),
		zap.String("user_word", userWord))

	if shiritoriSes.UsedWords == nil {
		shiritoriSes.UsedWords = make(map[string]bool)
	}
	if shiritoriSes.LetterOffsets == nil {
		shiritoriSes.LetterOffsets = make(map[string]int)
	}

	isRepeat = false
	notMatch = false
	userFirstLetter := strings.ToLower(userWord[0:1])
	if shiritoriSes.LastLetter != "" && shiritoriSes.LastLetter != userFirstLetter {
		notMatch = true
		return isRepeat, notMatch, nil
	}

	isRepeat = shiritoriSes.UsedWords[userWord]
	if isRepeat {
		return isRepeat, notMatch, nil
	}

	shiritoriSes.LastLetter = botLastLetter
	shiritoriSes.UsedWords[userWord] = true
	shiritoriSes.UsedWords[botWord] = true
	shiritoriSes.LetterOffsets[userLastLetter] = offsetID

	shiritoriSes.UserCorrectWords += 1

	jsonData, err := json.Marshal(shiritoriSes)
	if err != nil {
		return isRepeat, notMatch, fmt.Errorf("%s: marshal json: %w", op, err)
	}

	if err := us.sm.SetUserCtx(state, jsonData); err != nil {
		return isRepeat, notMatch, fmt.Errorf("%s: set user state: %w", op, err)
	}

	return isRepeat, notMatch, nil
}
