// Package learn words.go calls learn-service grpc methods
// responsible for words gprc methods
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

// Word is a struct that represents a word
type Word struct {
	Word        string `json:"word"`
	Level       string `json:"level"`
	Explain     string `json:"explain"`
	FirstLetter string `json:"first_letter"`
	Serial      int    `json:"serial"`
}

// NewWords adds new words to the database
func (ls *LearnService) NewWords(msg, reqTrace string) (int32, error) {
	const op = "learn.NewWords"

	ls.log.Debug("New words request",
		zap.String("op", op),
		zap.Int("msg len", len(msg)),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	res, err := services.CallRPC(ls.cb, func() (*pb.NewWordsRes, error) {
		return ls.client.NewWords(ctx, &pb.NewWordsReq{
			JsonData:     msg,
			RequestTrace: reqTrace,
		})
	})
	if err != nil {
		return 0, fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ls.log.Debug("New words response",
		zap.String("op", op),
		zap.Int32("inserted", res.Inserted),
		zap.String("reqTrace", reqTrace))

	return res.Inserted, nil
}

// GetWords returns words from the database
func (ls *LearnService) GetWords(searchData, reqTrace string, limit int32, buf *[]Word) error {
	const op = "learn.GetWords"

	ls.log.Debug("Get words request",
		zap.String("op", op),
		zap.Int("searchData len", len(searchData)),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	res, err := services.CallRPC(ls.cb, func() (*pb.GetWordsRes, error) {
		return ls.client.GetWords(ctx, &pb.GetWordsReq{
			SearchData:   searchData,
			Limit:        limit,
			RequestTrace: reqTrace,
		})
	})
	if err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	dataBytes := unsafe.Slice(unsafe.StringData(res.Data), len(res.Data))
	if err := json.Unmarshal(dataBytes, buf); err != nil {
		return fmt.Errorf("%s: unmarshal words: %w", op, err)
	}

	ls.log.Debug("Get words response",
		zap.String("op", op),
		zap.Int("words len", len(*buf)),
		zap.String("reqTrace", reqTrace))

	return nil
}

// GetWordsWithTarget returns words from the database
// and whether user word in the database
func (ls *LearnService) GetWordsWithTarget(userWord, lastLetter, reqTrace string, offsetID int, buf *[]Word) (bool, error) {
	const op = "learn.GetWordsWithTarget"

	ls.log.Debug("Get words with target request",
		zap.String("op", op),
		zap.String("userWord", userWord),
		zap.String("lastLetter", lastLetter),
		zap.Int("offsetID", offsetID),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	res, err := services.CallRPC(ls.cb, func() (*pb.GetWordsWithTargetRes, error) {
		return ls.client.GetWordsWithTarget(ctx, &pb.GetWordsWithTargetReq{
			UserWord:     userWord,
			FirstLetter:  lastLetter,
			OffsetId:     int32(offsetID),
			RequestTrace: reqTrace,
		})
	})
	if err != nil {
		return false, fmt.Errorf("%s: rpc call: %w", op, err)
	}

	if !res.Found {
		return res.Found, nil
	}

	dataBytes := unsafe.Slice(unsafe.StringData(res.Data), len(res.Data))
	if err := json.Unmarshal(dataBytes, buf); err != nil {
		return false, fmt.Errorf("%s: unmarshal words: %w", op, err)
	}

	(*buf) = (*buf)[1:]

	if len(*buf) == 0 {
		return res.Found, nil
	}

	ls.log.Debug("Get words with target response",
		zap.String("op", op),
		zap.Int("words len", len(*buf)),
		zap.String("reqTrace", reqTrace))

	return res.Found, nil
}

// DelWord deletes a word from the database
func (ls *LearnService) DelWord(msg, reqTrace string) error {
	const op = "learn.DelWord"

	ls.log.Debug("Delete word request",
		zap.String("op", op),
		zap.Int("msg len", len(msg)),
		zap.String("reqTrace", reqTrace))

	ctx, cancel := context.WithTimeout(context.Background(), ls.ctxTimeout*time.Second)
	defer cancel()

	msgBytes := unsafe.Slice(unsafe.StringData(msg), len(msg))
	word, serial, has := bytes.Cut(msgBytes, []byte(" "))
	if !has {
		return fmt.Errorf("%s: cut message: invalid message structure", op)
	}
	wordStr := unsafe.String(unsafe.SliceData(word), len(word))
	serialStr := unsafe.String(unsafe.SliceData(serial), len(serial))
	serialInt, err := strconv.Atoi(serialStr)
	if err != nil {
		return fmt.Errorf("%s: atoi serial: invalid serial", op)
	}

	if _, err := services.CallRPC(ls.cb, func() (*pb.DelWordRes, error) {
		return ls.client.DelWord(ctx, &pb.DelWordReq{
			Word:         wordStr,
			Serial:       int32(serialInt),
			RequestTrace: reqTrace,
		})
	}); err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ls.log.Debug("Delete word successfully",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

// GetLastLetter returns the last letter of the word
// following the rules of the shiritori game
func (ls *LearnService) GetLastLetter(word string, buf *string) {
	word = strings.ToLower(word)

	if len(word) == 0 {
		*buf = ""
		return
	}

	for i := len(word) - 1; i >= 0; i-- {
		char := word[i]
		switch char {
		case 'e', 'j', 'q', 's':
			continue
		default:
			*buf = word[i : i+1]
			return
		}
	}

	*buf = ""
}
