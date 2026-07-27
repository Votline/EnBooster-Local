// Package rdb provides Redis database connection
// and related methods.
package rdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
	"unsafe"

	"aisrv/internal/utils"

	"github.com/go-redis/redis/v8"
)

type RDB struct {
	rdb     *redis.Client
	ctxTTL  time.Duration
	timeout time.Duration
}

func NewRDB() (*RDB, error) {
	const op = "rdb.NewRDB"

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_AI_ADDR"),
		Password: os.Getenv("REDIS_AI_PASSWORD"),
		DB:       0,
	})

	pingTimeout := utils.GetEnvInt("REDIS_PING_TIMEOUT", 10)
	ctxTTL := utils.GetEnvInt("REDIS_CTX_TTL", 10)
	timeout := utils.GetEnvInt("REDIS_TIMEOUT", 10)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pingTimeout)*time.Second)
	defer cancel()

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("%s: redis ping: %w", op, err)
	}

	r := &RDB{
		rdb:     rdb,
		ctxTTL:  time.Duration(ctxTTL) * time.Minute,
		timeout: time.Duration(timeout) * time.Second,
	}

	return r, nil
}

func (r *RDB) Close() error {
	return r.rdb.Close()
}

// GetUserContext gets user context from redis.
func (r *RDB) GetUserContext(uuid int64) ([]int, error) {
	const op = "rdb.GetUserContext"

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := fmt.Sprintf("user_context:%d", uuid)

	val, err := r.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: redis get: %w", op, err)
	}

	var uctx []int
	valBytes := unsafe.Slice(unsafe.StringData(val), len(val))
	if err := json.Unmarshal(valBytes, &uctx); err != nil {
		return nil, fmt.Errorf("%s: json.Unmarshal: %w", op, err)
	}

	return uctx, nil
}

// SetUserContext sets user context to redis.
func (r *RDB) SetUserContext(uuid int64, uctx []int) error {
	const op = "rdb.SetUserContext"

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	uctxJSON, err := json.Marshal(uctx)
	if err != nil {
		return fmt.Errorf("%s: json.Marshal: %w", op, err)
	}

	key := fmt.Sprintf("user_context:%d", uuid)

	if err := r.rdb.Set(ctx, key, uctxJSON, r.ctxTTL).Err(); err != nil {
		return fmt.Errorf("%s: redis set: %w", op, err)
	}

	return nil
}

func (r *RDB) ClearAIContext(uuid int64) error {
	const op = "rdb.ClearAIContext"

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := fmt.Sprintf("user_context:%d", uuid)

	if err := r.rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("%s: redis del: %w", op, err)
	}

	return nil
}
