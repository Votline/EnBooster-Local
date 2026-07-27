// Package db words.go contains operation with words psql table.
package db

import (
	"context"
	"fmt"
	"strconv"

	"learn/internal/parser"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
)

// NewWordsBulk insert many words to database.
func (d *DB) NewWordsBulk(ctx context.Context, words []parser.Word, reqTrace string) (int32, error) {
	const op = "db.NewWordsBulk"

	if len(words) == 0 {
		return 0, fmt.Errorf("%s: empty words", op)
	}

	insertBuilder := d.bd.Insert("words").
		Columns("word", "explain", "level", "first_letter")

	for _, word := range words {
		insertBuilder = insertBuilder.
			Values(word.Word, word.Explain, word.Level, word.FirstLetter)
	}

	d.log.Debug("Insert words",
		zap.Int("words len", len(words)),
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

	d.log.Debug("Successfully inserted words",
		zap.Int64("rows_affected", rowsAffected),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return int32(rowsAffected), nil
}

// GetWords update 'words' slice with words by level and serial.
func (d *DB) GetWords(ctx context.Context, searchData string, limit int32, words *[]parser.Word, reqTrace string) error {
	const op = "db.GetWords"

	query := d.bd.Select("word, explain, level, first_letter, serial").
		From("words")

	serial, err := strconv.Atoi(searchData)
	if err != nil {
		if len(searchData) == 1 {
			query = query.Where(sq.Eq{"first_letter": searchData})
		} else {
			query = query.Where(sq.Eq{"word": searchData})
		}
	} else {
		query = query.Where(sq.Eq{"serial": serial})
	}

	lim := uint64(limit)
	if limit <= 0 {
		lim = d.getLimit
	}

	query = query.Limit(lim)

	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("%s: create get words query: %w", op, err)
	}

	d.log.Debug("Get words",
		zap.String("query", sql),
		zap.String("searchData", searchData),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if err := d.db.SelectContext(ctx, words, sql, args...); err != nil {
		return fmt.Errorf("%s: exec get words query: %w", op, err)
	}

	d.log.Debug("Successfully get words",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return nil
}

// GetWordsWithTarget words by first letter with user word.
func (d *DB) GetWordsWithTarget(ctx context.Context, userWord, firstLetter string, offsetID, limit int32, words *[]parser.Word, reqTrace string) error {
	const op = "db.GetWordsWithTarget"

	lim := uint64(limit)
	if limit <= 0 {
		lim = d.getLimit
	}

	query, args, err := d.bd.Select("word, explain, level, first_letter, serial").
		From("words").
		Where(sq.Or{
			sq.And{
				sq.Eq{"first_letter": firstLetter},
				sq.Gt{"serial": offsetID},
			},
			sq.Eq{"word": userWord},
		}).
		OrderByClause(sq.Expr("CASE WHEN word = ? THEN 0 ELSE 1 END ASC, serial ASC", userWord)).
		Limit(lim + 1).ToSql()
	if err != nil {
		return fmt.Errorf("%s: create get words query: %w", op, err)
	}

	d.log.Debug("Get words",
		zap.String("query", query),
		zap.String("first_letter", firstLetter),
		zap.String("user_word", userWord),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if err := d.db.SelectContext(ctx, words, query, args...); err != nil {
		return fmt.Errorf("%s: exec get words query: %w", op, err)
	}

	d.log.Debug("Successfully get words",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return nil
}

// DelWords delete by word and serial.
func (d *DB) DelWords(ctx context.Context, level string, serial int32, reqTrace string) error {
	const op = "db.DelWords"

	query, args, err := d.bd.Delete("words").
		Where(sq.Eq{"word": level}).
		Where(sq.Eq{"serial": serial}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: create del words query: %w", op, err)
	}

	d.log.Debug("Delete words",
		zap.String("query", query),
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: exec del words query: %w", op, err)
	}

	d.log.Debug("Successfully deleted words",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return nil
}
