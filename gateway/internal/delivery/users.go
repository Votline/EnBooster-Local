// Package delivery users.go handle user messages and state
package delivery

import (
	"bytes"
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

var aiTextPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 512))
	},
}

func (h *WSHandler) handleUser(buf *string, auBuf *bytes.Buffer, uctx stm.UserContext, src chatMsg, reqTrace string) error {
	const op = "delivery.handleUser"

	h.log.Info("handle user content",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	msg := strings.ToLower(src.Text)
	if msg == "/stop" && uctx.State != stm.StateShiritori {
		if err := h.sm.UpdUserStateCtx(stm.StateNone); err != nil {
			return fmt.Errorf("%s: update state: %w", op, err)
		}
		*buf = "Successfully stopped"
		return nil
	}

	resetState := false
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
		userAnswer := src.Text
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

		if err := h.usrsrv.UpdStreak(correct, theme, taskSes.Counter+add, reqTrace); err != nil {
			h.log.Error("Failed to update streak",
				zap.String("op", op),
				zap.String("reqTrace", reqTrace),
				zap.Error(err))
		}

		if correct {
			if err := h.usrsrv.UpdTaskID(1, reqTrace); err != nil {
				h.log.Error("Failed to update task id",
					zap.String("op", op),
					zap.String("reqTrace", reqTrace),
					zap.Error(err))
			}
		}
		resetState = true
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

		h.lrnsrv.GetLastLetter(src.Text, lastLetter)
		if *lastLetter == "" {
			*buf = "Invalid word: " + src.Text
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

		found, err := h.lrnsrv.GetWordsWithTarget(src.Text, *lastLetter, reqTrace, offsetID, wordsPtr)
		if err != nil {
			return fmt.Errorf("%s: get words: %w", op, err)
		}

		// check if bot found any word and user word exists
		// AND increment all words counter
		if !found {
			if err := h.saveState(shirSes); err != nil {
				return fmt.Errorf("%s: save state: %w", op, err)
			}
			*buf = "Your word not found: " + src.Text
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
			&shirSes, src.Text, *lastLetter, botWord, *botLastLetter,
			botWordID, stm.StateShiritori)
		if err != nil {
			return fmt.Errorf("%s: update user shiritori ctx: %w", op, err)
		}

		// shiritori rules

		if repeat {
			*buf = "You already used this word: " + src.Text
			return nil
		}

		if notMatch {
			*buf = "First letter in your word doesn't match with last letter in previous word"
			return nil
		}

		*buf = "Word: " + botWord
	case stm.StateAiSetting:
		var newState int8 = stm.StateNone
		switch msg {
		case "ttt":
			newState = stm.StateTTT
		case "tts":
			newState = stm.StateTTS
		case "stt":
			newState = stm.StateSTT
		case "sts":
			newState = stm.StateSTS
		default:
			*buf = "Invalid choose: " + msg
			return nil
		}
		if err := h.sm.UpdUserStateCtx(newState); err != nil {
			return fmt.Errorf("%s: update state: %w", op, err)
		}
		*buf = "AI settings updated"
	case stm.StateTTT:
		h.conn.WriteJSON(chatMsg{
			Text:     "AI is generating text...",
			ReqTrace: reqTrace,
			IsMe:     false,
		})

		resBuf := aiTextPool.Get().(*bytes.Buffer)
		resBuf.Reset()
		defer aiTextPool.Put(resBuf)

		if err := h.generateText(uctx, src.Text, reqTrace, func(text string) {
			resBuf.WriteString(text)
			h.conn.WriteJSON(chatMsg{
				Text:     resBuf.String(),
				ReqTrace: reqTrace,
				IsMe:     false,
			})
		}); err != nil {
			return fmt.Errorf("%s: generate text: %w", op, err)
		}

		*buf = resBuf.String()
	case stm.StateTTS:
		if err := h.handleTTS(uctx, auBuf, src.Text, reqTrace); err != nil {
			return fmt.Errorf("%s: state tts: %w", op, err)
		}
		*buf = ""
	case stm.StateSTT:
		h.conn.WriteJSON(chatMsg{
			Text:     "AI is recognizing audio...",
			ReqTrace: reqTrace,
			IsMe:     false,
		})

		fullText := ""
		if err := h.handleVoice(src.OGGBytes, reqTrace, func(text string) {
			fullText = text
			h.conn.WriteJSON(chatMsg{
				Text:     fullText,
				ReqTrace: reqTrace,
				IsMe:     false,
			})
		}); err != nil {
			return fmt.Errorf("%s: state stt: %w", op, err)
		}

		*buf = fullText
	case stm.StateSTS:
		h.conn.WriteJSON(chatMsg{
			Text:     "AI is recognizing audio...",
			ReqTrace: reqTrace,
			IsMe:     false,
		})

		if err := h.handleVoice(src.OGGBytes, reqTrace, func(text string) {
			auBuf.WriteString(text)
		}); err != nil {
			return fmt.Errorf("%s: state sts: %w", op, err)
		}

		recognizedText := auBuf.String()
		auBuf.Reset()

		if err := h.handleTTS(uctx, auBuf, recognizedText, reqTrace); err != nil {
			return fmt.Errorf("%s: state sts: %w", op, err)
		}
		*buf = ""
	case stm.StateSetLangLevel:
		upper := strings.ToUpper(src.Text)
		if err := h.usrsrv.UpdLangLevel(upper, reqTrace); err != nil {
			return fmt.Errorf("%s: state setlanglevel: %w", op, err)
		}
		*buf = "Successfully changed level to " + upper
		resetState = true
	case stm.StateSetSysPrompt:
		if err := h.usrsrv.UpdSystemPrompt(src.Text, reqTrace); err != nil {
			return fmt.Errorf("%s: state setsysprompt: %w", op, err)
		}
		*buf = "Successfully changed prompt"
		resetState = true
	default:
		*buf = "no handled"
		resetState = true
	}

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	if resetState {
		if err := h.sm.UpdUserStateCtx(stm.StateNone); err != nil {
			h.log.Error("Failed to change state",
				zap.String("op", op),
				zap.String("reqTrace", reqTrace),
				zap.Error(err))
		}
	}

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

// generateText generates a text message from the given user message and
// yields the result to the given callback function.
func (h *WSHandler) generateText(uctx stm.UserContext, src, reqTrace string, yield func(text string)) error {
	const op = "delivery.generateText"

	var chatSes stm.ChattingSession
	if len(uctx.JSONData) > 0 {
		uctxData := unsafe.Slice(unsafe.StringData(uctx.JSONData), len(uctx.JSONData))
		if err := json.Unmarshal(uctxData, &chatSes); err != nil {
			return fmt.Errorf("%s: unmarshal: %w", op, err)
		}
	}
	sysPrompt := chatSes.SystemPrompt

	var builder strings.Builder
	if err := h.aisrv.GenerateText(src, sysPrompt, reqTrace, func(res []byte) {
		resStr := unsafe.String(unsafe.SliceData(res), len(res))
		builder.WriteString(resStr)
		yield(resStr)
		h.log.Debug("Save bot msg", zap.String("str", resStr))
	}); err != nil {
		return fmt.Errorf("%s: generate text: %w", op, err)
	}

	chatSes.LastMessage = builder.String()

	jsonData, err := json.Marshal(chatSes)
	if err != nil {
		return fmt.Errorf("%s: marshal json: %w", op, err)
	}

	if err := h.sm.UpdateUserDataCtx(jsonData); err != nil {
		return fmt.Errorf("%s: update user data: %w", op, err)
	}

	return nil
}

// handleTTS handles TTS messages, call AI service and yields
// the result to the given callback function.
func (h *WSHandler) handleTTS(uctx stm.UserContext, auBuf *bytes.Buffer, usrMsg, reqTrace string) error {
	const op = "router.user.handleTTS"

	h.conn.WriteJSON(chatMsg{
		Text:     "AI is generating text...",
		ReqTrace: reqTrace,
		IsMe:     false,
	})

	if err := h.generateText(uctx, usrMsg, reqTrace, func(text string) {
		auBuf.WriteString(text)
	}); err != nil {
		return fmt.Errorf("%s: generate text: %w", op, err)
	}

	generatedText := auBuf.String()
	auBuf.Reset()

	h.conn.WriteJSON(chatMsg{
		Text:     "Successfully generated text. AI is generating audio...",
		ReqTrace: reqTrace,
		IsMe:     false,
	})

	if err := h.aisrv.GenerateAudio(generatedText, reqTrace, func(audio []byte) {
		auBuf.Write(audio)
	}); err != nil {
		return fmt.Errorf("%s: generate audio: %w", op, err)
	}

	h.conn.WriteJSON(chatMsg{
		OGGBytes: auBuf.Bytes(),
		ReqTrace: reqTrace,
		IsMe:     false,
	})

	return nil
}

// handleVoice handles voice messages, call recognition service and yields
// the result to the given callback function.
func (h *WSHandler) handleVoice(oggBytes []byte, reqTrace string, yield func(text string)) error {
	const op = "router.user.handleVoice"

	h.log.Debug("Recognize audio request",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if err := h.aisrv.RecognizeAudio(oggBytes, reqTrace, yield); err != nil {
		return fmt.Errorf("%s: generate text: %w", op, err)
	}

	h.log.Debug("Recognize audio successfully",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return nil
}
