// Package db tasks.go contains operation with tasks psql table.
package db

import (
	"context"
	"fmt"

	"learn/internal/parser"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
)

// NewTaskBulk insert many tasks to database.
func (d *DB) NewTaskBulk(ctx context.Context, tasks []parser.Task, reqTrace string) (int32, error) {
	const op = "db.NewTaskBulk"

	if len(tasks) == 0 {
		return 0, fmt.Errorf("%s: empty tasks", op)
	}

	var curMaxPos int32
	curLvl := tasks[0].Level

	maxQuery, maxArgs, maxErr := d.bd.Select("COALESCE(MAX(position), 0)").
		From("tasks").
		Where(sq.Eq{"level": curLvl}).
		ToSql()
	if maxErr != nil {
		return 0, fmt.Errorf("%s: create max query: %w", op, maxErr)
	}

	d.log.Debug("Get max position",
		zap.String("level", curLvl),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if err := d.db.QueryRowContext(ctx, maxQuery, maxArgs...).Scan(&curMaxPos); err != nil {
		return 0, fmt.Errorf("%s: exec get max position: %w", op, err)
	}

	insertBuilder := d.bd.Insert("tasks").
		Columns("task", "level", "theme", "answer", "position")

	for _, task := range tasks {
		curMaxPos++
		insertBuilder = insertBuilder.
			Values(task.TaskData, task.Level, task.Theme, task.Answer, curMaxPos)
	}

	d.log.Debug("Insert tasks",
		zap.Int("tasks len", len(tasks)),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	query, args, err := insertBuilder.ToSql()
	if err != nil {
		return 0, fmt.Errorf("%s: create insert bulk query: %w", op, err)
	}

	res, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("%s: exec insert bulk query: %w", op, err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: get rows affected: %w", op, err)
	}

	d.log.Debug("Successfully inserted tasks",
		zap.Int64("rows_affected", rowsAffected),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return int32(rowsAffected), nil
}

// GetTasks returns tasks by level and position.
func (d *DB) GetTasks(ctx context.Context, level string, pos, limit int32, tasks *[]parser.Task, reqTrace string) error {
	const op = "db.GetTask"

	query := d.bd.Select("task, level, theme, answer, position").From("tasks")

	if level != "" {
		query = query.Where(sq.Eq{"level": level})
	}

	if pos >= 0 {
		query = query.Where(sq.Eq{"position": pos})
	}

	if limit > 0 {
		query = query.Limit(uint64(limit))
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("%s: create get task query: %w", op, err)
	}

	d.log.Debug("Get tasks",
		zap.String("query", sql),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if err := d.db.SelectContext(ctx, tasks, sql, args...); err != nil {
		return fmt.Errorf("%s: exec get task query: %w", op, err)
	}

	d.log.Debug("Successfully get tasks",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return nil
}

func (d *DB) DelTask(ctx context.Context, level string, pos int32, reqTrace string) error {
	const op = "db.DelTask"

	query, args, err := d.bd.Delete("tasks").
		Where(sq.Eq{"level": level}).
		Where(sq.Eq{"position": pos}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: create del task query: %w", op, err)
	}

	d.log.Debug("Delete task",
		zap.String("level", level),
		zap.Int32("position", pos),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: exec del task query: %w", op, err)
	}

	d.log.Debug("Successfully deleted task",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return nil
}
