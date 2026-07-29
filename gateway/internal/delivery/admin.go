// Package delivery admin.go handle admin messages and state
package delivery

import (
	"fmt"
	"strings"

	stm "gateway/internal/statemanager"

	"go.uber.org/zap"
)

// helpMsg is a help message for admin commands
const helpMsg = `
- tasks_add - add task. Format json:
    * [{"task":" <full task message> ","level":"<english level>","answer": "<answer(s)>" }]
- task_del - delete task. Format message:
    * <level> <position>
- words_add - add word. Format json:
    * [{"word":"<word>","explain":"<explain>","level":"<english level>","first_letter":"<first letter>"}]
- word_del - delete word. Format message:
    * <word> <serial number>
`

// handleAdmin check message for admin commands or states
// set 'buf' value to 'no handled' on default case
func (h *WSHandler) handleAdmin(buf *string, uctx stm.UserContext, src, reqTrace string) error {
	const op = "delivery.handleAdmin"

	h.log.Info("handle admin content",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	*buf = ""
	stateNone := false
	msg := strings.ToLower(src)
	switch msg {
	case "tasks_add":
		if err := h.sm.UpdUserStateCtx(stm.StateTasksAdding); err != nil {
			return fmt.Errorf("%s: change state: %w", op, err)
		}
	case "words_add":
		if err := h.sm.UpdUserStateCtx(stm.StateWordsAdding); err != nil {
			return fmt.Errorf("%s: change state: %w", op, err)
		}
	case "task_del":
		if err := h.sm.UpdUserStateCtx(stm.StateWordsAdding); err != nil {
			return fmt.Errorf("%s: change state: %w", op, err)
		}
	case "word_del":
		if err := h.sm.UpdUserStateCtx(stm.StateWordsAdding); err != nil {
			return fmt.Errorf("%s: change state: %w", op, err)
		}
	case "help":
		*buf = helpMsg
	default:
		switch uctx.State {
		case stm.StateTasksAdding:
			stateNone = true
			inserted, err := h.lrnsrv.NewTasks(src, reqTrace)
			if err != nil {
				return fmt.Errorf("%s: new tasks: %w", op, err)
			}
			*buf = fmt.Sprintf("Successfully added %d tasks", inserted)
		case stm.StateWordsAdding:
			stateNone = true
			inserted, err := h.lrnsrv.NewWords(src, reqTrace)
			if err != nil {
				return fmt.Errorf("%s new words: %w", op, err)
			}
			*buf = fmt.Sprintf("Successfully added %d words", inserted)
		case stm.StateTaskDeleting:
			stateNone = true
			if err := h.lrnsrv.DelTask(src, reqTrace); err != nil {
				return fmt.Errorf("%s: delete task: %w", op, err)
			}
			*buf = "Successfully deleted task"
		case stm.StateWordDeleting:
			stateNone = true
			if err := h.lrnsrv.DelWord(src, reqTrace); err != nil {
				return fmt.Errorf("%s: delete word: %w", op, err)
			}
			*buf = "Successfully deleted word"
		default:
			*buf = "no handled"
			return nil
		}
	}
	if *buf == "" {
		*buf = "Send your data in json format"
	}

	if stateNone {
		if err := h.sm.UpdUserStateCtx(stm.StateNone); err != nil {
			return fmt.Errorf("%s: update user state: %w", op, err)
		}
	}

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}
