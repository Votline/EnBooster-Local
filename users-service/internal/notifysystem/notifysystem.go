// Package notifysystem implement notification sysytem
// send notifications to users in hardcode time
package notifysystem

import (
	"context"
	"fmt"
	"time"

	"users/internal/db"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// NotificationSystem strct contains
// instances for notification system
type NotificationSystem struct {
	db           *db.DB
	log          *zap.Logger
	writer       *kafka.Writer
	notifyTicker *time.Ticker
}

func NewNS(db *db.DB, wrt *kafka.Writer, log *zap.Logger) *NotificationSystem {
	ticker := time.NewTicker(time.Second)
	ticker.Stop()

	return &NotificationSystem{
		db:           db,
		log:          log,
		writer:       wrt,
		notifyTicker: ticker,
	}
}

// Scheduler trying to send notifications to users
// every hour in a 'slots' var
func (ns *NotificationSystem) Scheduler(ctx context.Context) {
	const op = "usersserver.scheduler"
	slots := []int{6, 9, 13, 15, 17, 19} // Moscow minus 3 (UTC)
	chatIds := make([]int64, 0, 20)
	msgs := make([]kafka.Message, 0, 20)
	bufs := make([][]byte, 20)

	for {
		now := time.Now().UTC()
		nextRun := getNextSlotTime(now, slots)
		sleepDuration := time.Until(nextRun)
		ns.log.Info("Next notification push",
			zap.Time("at", nextRun),
			zap.Duration("sleep", sleepDuration),
			zap.String("op", op))

		timer := time.NewTimer(sleepDuration)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := ns.notify(ctx, bufs, chatIds, msgs); err != nil {
				ns.log.Error("Failed to push notification",
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

// notify get users from db and push chat ids to kafka
func (ns *NotificationSystem) notify(ctx context.Context, bufs [][]byte, chatIds []int64, msgs []kafka.Message) error {
	const op = "usersserver.notify"

	ns.notifyTicker.Reset(time.Second)
	defer ns.notifyTicker.Stop()

	var lastID int32 = 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		chatIds = chatIds[:0]
		if err := ns.db.GetUsersByID(ctx, lastID, &chatIds); err != nil {
			return fmt.Errorf("%s: get chat ids: %w", op, err)
		}

		if len(chatIds) == 0 {
			break
		}

		lastID = int32(chatIds[len(chatIds)-1])

		if err := ns.sendNotificationBatch(ctx, bufs, chatIds, msgs); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}

		if len(chatIds) < cap(chatIds) {
			break
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ns.notifyTicker.C:
		}
	}

	return nil
}

// sendNotificationBatch create kafka messages and write it to writer
func (ns *NotificationSystem) sendNotificationBatch(ctx context.Context, bufs [][]byte, chatIDs []int64, msgs []kafka.Message) error {
	const op = "usersserver.sendNotificationBatch"

	msgs = msgs[:0]
	for i, chatID := range chatIDs {
		buf := (bufs[i])[:0]
		itoa(chatID, &buf)
		msgs = append(msgs, kafka.Message{
			Key:   buf,
			Value: buf,
		})
	}

	if err := ns.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("%s: write to kafka: %w", op, err)
	}

	return nil
}

// itoa converts int64 to []byte inside buf.
// Returns length of result.
func itoa(n int64, buf *[]byte) {
	if n == 0 {
		if len(*buf) > 0 {
			(*buf)[0] = '0'
		} else {
			*buf = append(*buf, '0')
		}
		return
	}

	var b [20]byte
	pos := len(b)

	isNeg := n < 0
	if isNeg {
		n = -n
	}

	for n > 0 {
		pos--
		b[pos] = byte('0' + (n % 10))
		n /= 10
	}

	if isNeg {
		pos--
		b[pos] = '-'
	}

	length := len(b) - pos

	if cap(*buf) < length {
		*buf = make([]byte, length)
	} else {
		*buf = (*buf)[:length]
	}

	copy(*buf, b[pos:])
}
