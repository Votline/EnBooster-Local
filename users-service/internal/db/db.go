// Package db provides database access for users service grpc methods.
package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

type DB struct {
	db  *sqlx.DB
	log *zap.Logger
	bd  sq.StatementBuilderType
}

// User is a overall structure for any operations with user
type User struct {
	UUID              int64  `db:"uuid" json:"uuid"`
	Level             string `db:"level" json:"level"`
	TaskID            int32  `db:"task_id" json:"task_id"`
	BestTheme         string `db:"best_theme" json:"best_theme"`
	BestThemeCounter  int    `db:"best_theme_counter" json:"best_theme_counter"`
	WorstTheme        string `db:"worst_theme" json:"worst_theme"`
	WorstThemeCounter int    `db:"worst_theme_counter" json:"worst_theme_counter"`
	Streak            int32  `db:"streak" json:"streak"`
	SystemPrompt      string `db:"system_prompt" json:"system_prompt"`
}

func GetEnvInt(key string, defaultVal int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

// NewDB creates new database connection.
func NewDB(log *zap.Logger) (*DB, error) {
	const op = "db.New"

	db, err := sqlx.Connect("postgres", os.Getenv("POSTGRES_URL"))
	if err != nil {
		return nil, fmt.Errorf("%s: sqlx connect: %w", op, err)
	}

	db.SetMaxOpenConns(GetEnvInt("MAX_OPEN_CONNS", 15))
	db.SetMaxIdleConns(GetEnvInt("MAX_IDLE_CONNS", 10))
	db.SetConnMaxLifetime(time.Duration(GetEnvInt("MAX_LIFETIME", 15)) * time.Minute)
	db.SetConnMaxIdleTime(time.Duration(GetEnvInt("MAX_IDLETIME", 10)) * time.Minute)

	log.Debug("DB users succesfully connected")

	return &DB{
		db:  db,
		log: log,
		bd:  sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

// RegUser add user to database.
func (d *DB) RegUser(uuid, chatID int64, ctx context.Context, reqTrace string) error {
	const op = "db.RegUser"

	query, args, err := d.bd.Insert("users").
		Columns("uuid", "chat_id").
		Values(uuid, chatID).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build insert query: %w", op, err)
	}

	d.log.Debug("RegUser query",
		zap.String("query", query),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: insert user: %w", op, err)
	}

	d.log.Debug("User succesfully registered",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return nil
}

// GetUser get user from database.
// Returns all user fields if user exists
func (d *DB) GetUser(uuid int64, ctx context.Context, reqTrace string) (*User, error) {
	const op = "db.GetUser"

	query, args, err := d.bd.Select(
		"level", "task_id", "best_theme",
		"best_theme_counter", "worst_theme",
		"worst_theme_counter", "streak", "system_prompt").
		From("users").
		Where(sq.Eq{"uuid": uuid}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build get query: %w", op, err)
	}

	d.log.Debug("GetUser request",
		zap.String("query", query),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	var user User
	if err := d.db.GetContext(ctx, &user, query, args...); err != nil {
		return nil, fmt.Errorf("%s: get user: %w", op, err)
	}

	d.log.Debug("succesfully get user",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return &user, nil
}

// GetUsersByID get 20 chat_id by 'id' - SERIAL field
func (d *DB) GetUsersByID(ctx context.Context, id int32, chatBuf *[]int64) error {
	const op = "db.GetUsersByID"

	currentDay := time.Now().UTC().Unix() / 86400

	query, args, err := d.bd.Select("chat_id").
		From("users").
		Where(sq.Gt{"id": id}).
		Where(sq.Lt{"last_done_day": currentDay}).
		OrderBy("id ASC").
		Limit(20).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build get query", op)
	}

	if err := d.db.SelectContext(ctx, chatBuf, query, args...); err != nil {
		return fmt.Errorf("%s: get users: %w", op, err)
	}

	return nil
}

// UpdateStreak atomically updates the streak of a user
func (d *DB) UpdateStreak(uuid int64, ctx context.Context, reqTrace string, correct bool, theme string, counter int) error {
	const op = "db.UpdateStreak"

	currentDay := time.Now().UTC().Unix() / 86400

	streakCase := sq.Expr(`CASE
		WHEN last_done_day = ? THEN streak
		WHEN last_done_day = ? THEN streak + 1
		ELSE 1
	END`, currentDay, currentDay-1)

	bestThemeCntCase := sq.Expr(`CASE
		WHEN ? > best_theme_counter THEN ?
		ELSE best_theme_counter
	END`, counter, counter)

	bestThemeCase := sq.Expr(`CASE
		WHEN ? > best_theme_counter THEN ?
		ELSE best_theme
	END`, counter, theme)

	worstThemeCntCase := sq.Expr(`CASE
		WHEN ? < worst_theme_counter THEN ?
		ELSE worst_theme_counter
	END`, counter, counter)

	worstThemeCase := sq.Expr(`CASE
		WHEN ? < worst_theme_counter THEN ?
		ELSE worst_theme
	END`, counter, theme)

	query, args, err := d.bd.Update("users").
		Set("streak", streakCase).
		Set("best_theme", bestThemeCase).
		Set("best_theme_counter", bestThemeCntCase).
		Set("worst_theme", worstThemeCase).
		Set("worst_theme_counter", worstThemeCntCase).
		Set("last_done_day", currentDay).
		Where(sq.Eq{"uuid": uuid}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build update query: %w", op, err)
	}

	d.log.Debug("UpdateStreak query",
		zap.String("query", query),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: update streak: %w", op, err)
	}

	d.log.Debug("Streak succesfully updated",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return nil
}

// UpdateSystemPrompt updates the system prompt of a user
func (d *DB) UpdateSystemPrompt(ctx context.Context, uuid int64, sp, reqTrace string) error {
	const op = "db.UpdateSystemPrompt"

	query, args, err := d.bd.Update("users").
		Set("system_prompt", sp).
		Where(sq.Eq{"uuid": uuid}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build update query: %w", op, err)
	}

	d.log.Debug("UpdateSystemPrompt query",
		zap.String("query", query),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: update system prompt: %w", op, err)
	}

	d.log.Debug("SystemPrompt succesfully updated",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return nil
}

func (d *DB) UpdateLangLevel(ctx context.Context, uuid int64, level string, reqTrace string) error {
	const op = "db.UpdateLangLevel"

	query, args, err := d.bd.Update("users").
		Set("level", level).
		Where(sq.Eq{"uuid": uuid}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build update query: %w", op, err)
	}

	d.log.Debug("UpdateLangLevel query",
		zap.String("query", query),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: update lang level: %w", op, err)
	}

	d.log.Debug("LangLevel succesfully updated",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return nil
}

// DelUser delete user from database
func (d *DB) DelUser(uuid int64, ctx context.Context, reqTrace string) error {
	const op = "db.DelUser"

	query, args, err := d.bd.Delete("users").
		Where(sq.Eq{"uuid": uuid}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build delete query: %w", op, err)
	}

	d.log.Debug("DelUser query",
		zap.String("query", query),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: delete user: %w", op, err)
	}

	d.log.Debug("User succesfully deleted",
		zap.Int64("uuid", uuid),
		zap.String("request_trace", reqTrace),
		zap.String("op", op))

	return nil
}
