// Package delivery make connect with frontend
// catch messages and send bot answer
package delivery

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gateway/internal/learn"
	"gateway/internal/statemanager"
	"gateway/internal/users"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WSHandler main struct for BFF connection
type WSHandler struct {
	log      *zap.Logger
	upgrader *websocket.Upgrader
	sm       *statemanager.StateManager
	lrnsrv   *learn.LearnService
	usrsrv   *users.UsersService
}

// chatMsg used for send/accept message from frontend
type chatMsg struct {
	UUID string `json:"uuid"`
	Text string `json:"text"`
	IsMe bool   `json:"is_me"`
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

// NewWSH creates new WSHandler instance
func NewWSH(log *zap.Logger) (*WSHandler, error) {
	const op = "delivery.NewWSH"
	ctxtimeout := time.Duration(GetEnvInt("CTX_TIMEOUT", 10)) * time.Second
	statettl := time.Duration(GetEnvInt("STATE_TTL", 30)) * time.Minute
	pingtimeout := time.Duration(GetEnvInt("PING_TIMEOUT", 10)) * time.Second

	stmngr, err := statemanager.NewSM(ctxtimeout, statettl, pingtimeout)
	if err != nil {
		return nil, fmt.Errorf("%s: create statemanager: %w", op, err)
	}

	usrsrv, err := users.NewUS(ctxtimeout, stmngr, log)
	if err != nil {
		return nil, fmt.Errorf("%s: create users-service: %w", op, err)
	}

	log.Info("Connected to users-service", zap.String("op", op))

	lrnsrv, err := learn.NewLS(stmngr, ctxtimeout, log)
	if err != nil {
		return nil, fmt.Errorf("%s: create learn-service: %w", op, err)
	}

	log.Info("Connected to learn-service", zap.String("op", op))

	return &WSHandler{
		log: log,
		upgrader: &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		sm:     stmngr,
		lrnsrv: lrnsrv,
		usrsrv: usrsrv,
	}, nil
}

// RegisterRoutes register routes for WSHandler
func (h *WSHandler) RegisterRoutes(s *http.ServeMux) {
	s.HandleFunc("/ws", h.HandleChatWS)
}

// HandleChatWS upgrade conection to websocket and
// handle messages
func (h *WSHandler) HandleChatWS(w http.ResponseWriter, r *http.Request) {
	const op = "delivery.HandleChatWS"
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("WS upgrade failed",
			zap.String("op", op),
			zap.Error(err))
		http.Error(w, "WS upgrade failed", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	h.log.Info("Frontend successfully connceted", zap.String("op", op))

	var answer string
	for {
		msg := chatMsg{}
		if err := conn.ReadJSON(&msg); err != nil {
			h.log.Error("WS read failed",
				zap.String("op", op),
				zap.Error(err))
			break
		}

		h.log.Info("Accepted message",
			zap.String("text", msg.Text),
			zap.String("op", op))

		answer = ""
		h.handleMessage(msg.Text, &answer)

		reply := chatMsg{
			UUID: uuid.NewString(),
			Text: "Bot accepted: " + answer,
			IsMe: false,
		}

		if err := conn.WriteJSON(reply); err != nil {
			h.log.Error("WS write failed",
				zap.String("op", op),
				zap.Error(err))
			break
		}
	}
}

// tasksPool used for getting tasks
var tasksPool = sync.Pool{
	New: func() any {
		buf := make([]learn.Task, 0, 1)
		return &buf
	},
}

// handleMessage handles incoming messages
// and call needed services
func (h *WSHandler) handleMessage(src string, buf *string) error {
	const op = "delivery.handleUser"

	reqTrace := uuid.NewString()
	msg := strings.ToLower(src)

	h.log.Info("New request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace),
		zap.Int("msg_len", len(msg)))

	switch msg {
	case "start":
		*buf = "Hello"
	case "learning":
		ud, err := h.usrsrv.GetData(reqTrace)
		if err != nil {
			return fmt.Errorf("%s: get user data: %w", op, err)
		}

		h.log.Info("Successfully get user data",
			zap.String("op", op),
			zap.String("reqTrace", reqTrace))

		taskBuf := tasksPool.Get().(*[]learn.Task)
		defer tasksPool.Put(taskBuf)

		if err := h.lrnsrv.GetTasks(ud.Level, ud.TaskID, 1, taskBuf, reqTrace); err != nil {
			return fmt.Errorf("%s: get tasks: %w", op, err)
		}

		*buf = "No tasks found"
		if len(*taskBuf) != 0 {
			*buf = (*taskBuf)[0].TaskData
		}

		h.log.Info("Get task",
			zap.String("op", op),
			zap.String("reqTrace", reqTrace),
			zap.Int("task_len", len(*buf)))
	}

	h.log.Info("Request successfully processed",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}
