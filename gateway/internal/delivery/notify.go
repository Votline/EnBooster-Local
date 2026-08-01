// Package delivery notify.go implemnt notification system
// send notification to user
package delivery

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (h *WSHandler) Scheduler(ctx context.Context) {
	const op = "delivery.scheduler"
	slots := []int{6, 9, 13, 15, 17, 19}

	for {
		now := time.Now().UTC()
		nextRun := getNextSlotTime(now, slots)
		sleepDuration := time.Until(nextRun)
		h.log.Info("Next notification push",
			zap.Time("at", nextRun),
			zap.Duration("sleep", sleepDuration),
			zap.String("op", op))

		timer := time.NewTimer(sleepDuration)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := h.notify(); err != nil {
				h.log.Error("Failed to push notification",
					zap.Error(err),
					zap.String("op", op))
			}
		}
	}
}

// getNextSlotTime calculate next hour to push notification
func getNextSlotTime(now time.Time, slots []int) time.Time {
	curHour := now.Hour()

	for _, slot := range slots {
		if slot > curHour {
			return time.Date(
				now.Year(), now.Month(), now.Day(),
				slot, 0, 0, 0, now.Location())
		}
	}

	tomorrow := now.AddDate(0, 0, 1)
	return time.Date(
		tomorrow.Year(), tomorrow.Month(), tomorrow.Day(),
		slots[0], 0, 0, 0, now.Location())
}

// notify checks user task status and pushes notification if missed today
func (h *WSHandler) notify() error {
	const op = "delivery.notify"

	reqTrace := uuid.NewString()

	ud, err := h.usrsrv.GetData(reqTrace)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	dayNow := time.Now().UTC().Unix() / 86400

	if ud.LastDoneDay >= dayNow {
		return nil
	}

	if err := h.sendNotification(reqTrace); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// sendNotification send message to user
func (h *WSHandler) sendNotification(reqTrace string) error {
	const op = "delivery.sendNotification"

	msgs := h.usrsrv.NotifyMsgs
	if len(msgs) == 0 {
		return nil
	}

	rIdx := rand.N(len(msgs))
	rMsg := msgs[rIdx]

	reply := chatMsg{
		Text:     rMsg,
		ReqTrace: reqTrace,
	}

	if h.conn != nil {
		if err := h.conn.WriteJSON(reply); err != nil {
			h.log.Error("WS write failed",
				zap.String("op", op),
				zap.Error(err))
			return fmt.Errorf("%s: ws write: %w", op, err)
		}
	}

	return nil
}
