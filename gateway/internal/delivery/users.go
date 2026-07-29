// Package delivery users.go handle user messages and state
package delivery

import (
	"encoding/json"
	"fmt"
	"unsafe"

	stm "gateway/internal/statemanager"

	"go.uber.org/zap"
)

func (h *WSHandler) handleUser(buf *string, uctx stm.UserContext, msg, reqTrace string) error {
	const op = "delivery.handleUser"

	h.log.Info("handle admin content",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

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
		userAnswer := msg
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
	}

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}
