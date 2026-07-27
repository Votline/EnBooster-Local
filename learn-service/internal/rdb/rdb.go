// Package rdb provides Redis database connection and
// methods for caching data.
package rdb

import (
	"context"
	"fmt"
	"os"
	"time"
	"unsafe"

	"learn/internal/db"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/singleflight"
)

type RDB struct {
	rdb      *redis.Client
	sfGroup  singleflight.Group
	countTTL int
	taskTTL  time.Duration
}

func NewRDB() (*RDB, error) {
	const op = "rdb.NewRDB"

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_LRN_ADDR"),
		Password: os.Getenv("REDIS_LRN_PASSWORD"),
		DB:       0,
	})

	pingTimeout := db.GetEnvInt("REDIS_PING_TIMEOUT", 10)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pingTimeout)*time.Second)
	defer cancel()

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("%s: redis ping: %w", op, err)
	}

	countTTL := db.GetEnvInt("COUNT_TTL", 1800)
	taskTTL := db.GetEnvInt("TASK_TTL", 30)

	r := &RDB{
		rdb:      rdb,
		sfGroup:  singleflight.Group{},
		countTTL: countTTL,
		taskTTL:  time.Duration(taskTTL),
	}

	return r, nil
}

func (r *RDB) Close() error {
	return r.rdb.Close()
}

var incrAndExpire = redis.NewScript(`
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return current
`)

func (r *RDB) CacheTasks(ctx context.Context, bufCount, bufCache *[]byte, value string) error {
	const op = "rdb.CacheTasks"

	BuildKey("tasks:count:", bufCount)
	countKey := unsafe.String(unsafe.SliceData(*bufCount), len(*bufCount))

	BuildKey("tasks:cache:", bufCache)
	cacheKey := unsafe.String(unsafe.SliceData(*bufCache), len(*bufCache))

	count, err := incrAndExpire.Run(ctx, r.rdb, []string{countKey}, r.countTTL).Int64()
	if err != nil {
		return fmt.Errorf("%s: redis incr and expire: %w", op, err)
	}

	if count < 10 {
		return nil
	}

	if err := r.rdb.Set(ctx, cacheKey, value, r.taskTTL*time.Minute).Err(); err != nil {
		return fmt.Errorf("%s: redis set: %w", op, err)
	}

	if err := r.rdb.Del(ctx, countKey).Err(); err != nil {
		return fmt.Errorf("%s: redis del: %w", op, err)
	}

	return nil
}

func (r *RDB) GetTasks(ctx context.Context, key *[]byte, sfKey string) (string, error) {
	const op = "rdb.GetTasks"

	val, err, _ := r.sfGroup.Do(sfKey, func() (any, error) {
		BuildKey("tasks:cache:", key)
		cacheKey := unsafe.String(unsafe.SliceData(*key), len(*key))

		tasks, err := r.rdb.Get(ctx, cacheKey).Result()
		if err != nil {
			return "", fmt.Errorf("%s: redis get: %w", op, err)
		}

		return tasks, nil
	})

	if err != nil {
		return "", fmt.Errorf("%s: get tasks: %w", op, err)
	}

	return val.(string), nil
}

func (r *RDB) DelTask(ctx context.Context, bufCache, bufCount *[]byte) error {
	const op = "rdb.DelTask"

	BuildKey("tasks:count:", bufCount)
	countKey := unsafe.String(unsafe.SliceData(*bufCount), len(*bufCount))

	BuildKey("tasks:cache:", bufCache)
	cacheKey := unsafe.String(unsafe.SliceData(*bufCache), len(*bufCache))

	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, cacheKey)
	pipe.Del(ctx, countKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%s: redis pipeline: %w", op, err)
	}

	return nil
}

// BuildKey add prefix to key.
func BuildKey(prefix string, key *[]byte) {
	origLen := len(*key)
	prefLen := len(prefix)
	totalLen := prefLen + origLen

	*key = (*key)[:totalLen]

	copy((*key)[prefLen:], (*key)[:origLen])
	copy((*key)[:prefLen], prefix)
}
