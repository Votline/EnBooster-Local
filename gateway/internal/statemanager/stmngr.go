// Package statemanager contains states for each user
// and helps methods to work with states
package statemanager

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"
)

// States are the possible states of the user
const (
	StateNone = iota
	StateTaskAdding
	StateTaskDeleting
	StateWordAdding
	StateWordDeleting
	StateTaskLearning
	StateAdminNotCommand
	StateShiritori
	StateSetSysPrompt
	StateSetLangLevel
	StateTTS
	StateSTT
	StateTTT
	StateSTTandTTS
	StateTranscriptVoice
	StateAiSetting
)

// StateManager is a struct that manages the state of the user
type StateManager struct {
	rdb        *redis.Client
	ctxTimeout time.Duration
	stateTTL   time.Duration
}

// TaskSession used for handle user results
// needed for best/worst theme and count they
type TaskSession struct {
	CurrentTheme string `json:"current_theme"`
	Counter      int    `json:"counter"`
	Answer       string `json:"answer"`
}

// ShiritoriSession helps check if the user used the word
type ShiritoriSession struct {
	UsedWords        map[string]bool `json:"used_words"`
	LastLetter       string          `json:"last_letter"`
	LetterOffsets    map[string]int  `json:"letter_offsets"`
	AllWords         uint            `json:"all_words"`
	UserWords        uint            `json:"user_words"`
	UserCorrectWords uint            `json:"user_correct_words"`
}

type ChattingSession struct {
	SystemPrompt string `json:"system_prompt"`
	LastMessage  string `json:"last_message"`
}

// UserContext contains user state and additional data
type UserContext struct {
	State    int8
	JSONData string
}

// NewSM connects to redis and returns a new StateManager
func NewSM(ctxTimeout time.Duration, stateTTL, pingTimeout time.Duration) (*StateManager, error) {
	const op = "statemanager.NewSM"

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_SM_ADDR"),
		Password: os.Getenv("REDIS_SM_PASSWORD"),
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("%s: ping: %w", op, err)
	}

	return &StateManager{
		rdb:        rdb,
		ctxTimeout: ctxTimeout,
		stateTTL:   stateTTL,
	}, nil
}

// Close closes the connection to redis
func (sm *StateManager) Close() error {
	return sm.rdb.Close()
}

// GetUserCtx returns the user context of the user from redis
func (sm *StateManager) GetUserCtx(uuid int64) (UserContext, error) {
	const op = "statemanager.GetState"

	ctx, cancel := context.WithTimeout(context.Background(), sm.ctxTimeout)
	defer cancel()

	key := "users:state" + strconv.FormatInt(uuid, 10)

	res, err := sm.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return UserContext{State: StateNone}, fmt.Errorf("%s: get state: %w", op, err)
	}

	if len(res) == 0 {
		return UserContext{State: StateNone}, nil
	}

	stateInt, err := strconv.ParseInt(res["state"], 10, 8)
	if err != nil {
		return UserContext{State: StateNone}, fmt.Errorf("%s: parse state: %w", op, err)
	}

	return UserContext{
		State:    int8(stateInt),
		JSONData: res["json_data"],
	}, nil
}

// SetUserCtx sets the state and data of the user in redis
func (sm *StateManager) SetUserCtx(uuid int64, state int8, jsonData []byte) error {
	const op = "statemanager.SetState"

	jsonStr := unsafe.String(unsafe.SliceData(jsonData), len(jsonData))

	ctx, cancel := context.WithTimeout(context.Background(), sm.ctxTimeout)
	defer cancel()

	key := "users:state" + strconv.FormatInt(uuid, 10)

	dataMap := map[string]string{
		"state":     strconv.FormatInt(int64(state), 10),
		"json_data": jsonStr,
	}

	pipe := sm.rdb.Pipeline()
	pipe.HMSet(ctx, key, dataMap)
	pipe.Expire(ctx, key, sm.stateTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%s: set state: %w", op, err)
	}

	return nil
}

var updateonlystate = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	redis.call("HSET", KEYS[1], "state", ARGV[1])
	redis.call("EXPIRE", KEYS[1], ARGV[2])
	return 1
else
	redis.call("HSET", KEYS[1], "state", ARGV[1], "json_data", "")
	redis.call("EXPIRE", KEYS[1], ARGV[2])
	return 2
end
`)

func (sm *StateManager) UpdUserStateCtx(uuid int64, state int8) error {
	const op = "statemanager.UpdUserStateCtx"

	ctx, cancel := context.WithTimeout(context.Background(), sm.ctxTimeout)
	defer cancel()

	key := "users:state" + strconv.FormatInt(uuid, 10)

	ttlSeconds := int64((sm.stateTTL).Seconds())

	res, err := updateonlystate.Run(ctx, sm.rdb, []string{key}, state, ttlSeconds).Result()
	if err != nil {
		return fmt.Errorf("%s: run script: %w", op, err)
	}

	if res == 0 {
		return fmt.Errorf("%s: state was not updated", op)
	}

	return nil
}

var updateonlyjsondata = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
    redis.call("HSET", KEYS[1], "json_data", ARGV[1])
    redis.call("EXPIRE", KEYS[1], ARGV[2])
    return 1
else
    redis.call("HSET", KEYS[1], "state", 0, "json_data", ARGV[1])
    redis.call("EXPIRE", KEYS[1], ARGV[2])
    return 2
end
`)

// UpdateUserDataCtx updates the user data in redis
func (sm *StateManager) UpdateUserDataCtx(uuid int64, jsonData []byte) error {
	const op = "statemanager.UpdateUserDataCtx"

	ctx, cancel := context.WithTimeout(context.Background(), sm.ctxTimeout)
	defer cancel()

	key := "users:state" + strconv.FormatInt(uuid, 10)

	ttlSeconds := int64((sm.stateTTL).Seconds())

	res, err := updateonlyjsondata.Run(ctx, sm.rdb, []string{key}, jsonData, ttlSeconds).Result()
	if err != nil {
		return fmt.Errorf("%s: run script: %w", op, err)
	}

	if res == 0 {
		return fmt.Errorf("%s: user data was not updated", op)
	}

	return nil
}
