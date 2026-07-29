// Package delivery users.go handle user messages and state
package delivery

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"gateway/internal/learn"
	stm "gateway/internal/statemanager"

	"go.uber.org/zap"
)

var lastLetterPool = sync.Pool{
	New: func() any {
		var s string
		return &s
	},
}

var wordsPool = sync.Pool{
	New: func() any {
		var w []learn.Word
		return &w
	},
}

func (h *WSHandler) handleUser(buf *string, uctx stm.UserContext, src, reqTrace string) error {
	const op = "delivery.handleUser"

	h.log.Info("handle admin content",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	msg := strings.ToLower(src)

	switch uctx.State {
	case stm.StateTaskLearning:
		var taskSes stm.TaskSession
		uctxData := unsafe.Slice(unsafe.StringData(uctx.JSONData), len(uctx.JSONData))
		if len(uctxData) > 0 {
			if err := json.Unmarshal(uctxData, &taskSes); err != nil {
				return fmt.Errorf("%s: unmarshal: %w", op, err)
			}
		}

		add := 0
		userAnswer := src
		answer := taskSes.Answer
		theme := taskSes.CurrentTheme
		correct := h.lrnsrv.VerifyAnswer(userAnswer, answer, reqTrace)
		if correct {
			add = 1
			*buf = "Correct answer"
		} else {
			add = -1
			*buf = "Incorrect answer. Correct variant: " + answer
		}

		if err := h.usrsrv.UpdateUserTaskCtx(uctx, theme, answer, reqTrace, add); err != nil {
			h.log.Error("Failed to update user task ctx",
				zap.String("op", op),
				zap.String("reqTrace", reqTrace),
				zap.Error(err))
		}
	case stm.StateShiritori:
		var shirSes stm.ShiritoriSession
		uctxData := unsafe.Slice(unsafe.StringData(uctx.JSONData), len(uctx.JSONData))
		if len(uctxData) > 0 {
			if err := json.Unmarshal(uctxData, &shirSes); err != nil {
				return fmt.Errorf("%s: unmarshal: %w", op, err)
			}
		}

		if msg == "stop" {
			if err := h.sm.SetUserCtx(stm.StateNone, nil); err != nil {
				return fmt.Errorf("%s: change state: %w", op, err)
			}

			botWords := shirSes.AllWords - shirSes.UserWords
			incorrectWords := shirSes.UserWords - shirSes.UserCorrectWords

			res := fmt.Sprintf(
				"Shiritori game stopped.\n\n"+
					"Match Statistics: \n"+
					"Total words in game: %d\n"+
					"Bot words: %d\n"+
					"Your total attempts: %d\n"+
					"Your correct words: %d\n"+
					"Your mistakes: %d\n",
				shirSes.AllWords,
				botWords,
				shirSes.UserWords,
				shirSes.UserCorrectWords,
				incorrectWords,
			)

			*buf = res
			return nil
		}

		shirSes.AllWords++
		shirSes.UserWords++

		// get last letter of the user word
		lastLetter := lastLetterPool.Get().(*string)
		defer lastLetterPool.Put(lastLetter)

		h.lrnsrv.GetLastLetter(src, lastLetter)
		if *lastLetter == "" {
			*buf = "Invalid word: " + src
			return nil
		}

		// get offset of the last letter
		if shirSes.LetterOffsets == nil {
			shirSes.LetterOffsets = make(map[string]int)
		}
		offsetID := shirSes.LetterOffsets[*lastLetter]

		// get words with target
		wordsPtr := wordsPool.Get().(*[]learn.Word)
		defer wordsPool.Put(wordsPtr)

		found, err := h.lrnsrv.GetWordsWithTarget(src, *lastLetter, reqTrace, offsetID, wordsPtr)
		if err != nil {
			return fmt.Errorf("%s: get words: %w", op, err)
		}

		// check if bot found any word and user word exists
		// AND increment all words counter
		if !found {
			if err := h.saveState(shirSes); err != nil {
				return fmt.Errorf("%s: save state: %w", op, err)
			}
			*buf = "Your word not found: " + src
			return nil
		}

		if len(*wordsPtr) == 0 {
			if err := h.saveState(shirSes); err != nil {
				return fmt.Errorf("%s: save state: %w", op, err)
			}
			*buf = "Bot couldn't find any word"
			return nil
		} else {
			shirSes.AllWords += 1
		}

		botWord := (*wordsPtr)[0].Word
		botWordID := (*wordsPtr)[0].Serial

		botLastLetter := lastLetterPool.Get().(*string)
		defer lastLetterPool.Put(botLastLetter)

		h.lrnsrv.GetLastLetter(botWord, botLastLetter)
		if *botLastLetter == "" {
			return fmt.Errorf("%s: get last letter bot word: %s: %w", op, botWord, err)
		}

		// update user shiritori ctx with get bools for shiritori rules
		// inside the method also increment user CORRECT words
		repeat, notMatch, err := h.usrsrv.UpdateUserShiritoriCtx(
			&shirSes, src, *lastLetter, botWord, *botLastLetter,
			botWordID, stm.StateShiritori)
		if err != nil {
			return fmt.Errorf("%s: update user shiritori ctx: %w", op, err)
		}

		// shiritori rules

		if repeat {
			*buf = "You already used this word: " + src
			return nil
		}

		if notMatch {
			*buf = "First letter in your word doesn't match with last letter in previous word"
			return nil
		}

		*buf = "Word: " + botWord
	}

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

// saveState marshall shiritori session and
// update it in user context
func (h *WSHandler) saveState(shirSes stm.ShiritoriSession) error {
	const op = "delivery.saveState"

	jsonData, err := json.Marshal(shirSes)
	if err != nil {
		return fmt.Errorf("%s: marshal json: %w", op, err)
	}

	if err := h.sm.UpdateUserDataCtx(jsonData); err != nil {
		return fmt.Errorf("%s: update user data: %w", op, err)
	}

	return nil
}
