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

	"gateway/internal/ai"
	"gateway/internal/learn"
	stm "gateway/internal/statemanager"
	"gateway/internal/users"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WSHandler main struct for BFF connection
type WSHandler struct {
	log      *zap.Logger
	conn     *websocket.Conn
	upgrader *websocket.Upgrader
	sm       *stm.StateManager
	lrnsrv   *learn.LearnService
	usrsrv   *users.UsersService
	aisrv    *ai.AIService
}

// chatMsg used for send/accept message from frontend
type chatMsg struct {
	Text     string `json:"text"`
	ReqTrace string `json:"req_trace"`
}

// tasksPool used for getting tasks
var tasksPool = sync.Pool{
	New: func() any {
		buf := make([]learn.Task, 0, 1)
		return &buf
	},
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

	stmngr, err := stm.NewSM(ctxtimeout, statettl, pingtimeout)
	if err != nil {
		return nil, fmt.Errorf("%s: create stm: %w", op, err)
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

	aisrv, err := ai.NewAIS(ctxtimeout, stmngr, log)
	if err != nil {
		return nil, fmt.Errorf("%s: create ai-service: %w", op, err)
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
		aisrv:  aisrv,
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
	h.conn = conn

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
		reqTrace := uuid.NewString()
		if err := h.handleMessage(reqTrace, msg.Text, &answer); err != nil {
			answer = "Something went wrong. Try again later"
			h.log.Error("handleMessage failed",
				zap.String("op", op),
				zap.String("reqTrace", reqTrace),
				zap.Error(err))
		}

		reply := chatMsg{
			Text:     answer,
			ReqTrace: reqTrace,
		}

		if err := conn.WriteJSON(reply); err != nil {
			h.log.Error("WS write failed",
				zap.String("op", op),
				zap.Error(err))
			break
		}
	}
}

// handleMessage handles incoming messages
// and call needed services
func (h *WSHandler) handleMessage(reqTrace, src string, buf *string) error {
	const op = "delivery.handleUser"

	uctx, err := h.sm.GetUserCtx()
	if err != nil {
		return fmt.Errorf("%s: get user state: %w", op, err)
	}

	msg := strings.ToLower(src)

	h.log.Info("New request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace),
		zap.Int("msg_len", len(msg)))

	switch msg {
	case "start":
		*buf = "Hello"
	case "learning":
		if err := h.learningTask(buf, uctx, reqTrace); err != nil {
			return fmt.Errorf("%s: unexpected error: %w", op, err)
		}
	case "shiritori":
		if err := h.shiritori(buf, reqTrace); err != nil {
			return fmt.Errorf("%s: unexpected error: %w", op, err)
		}
	case "chatting":
		if err := h.chatting(buf, reqTrace); err != nil {
			return fmt.Errorf("%s: unexpected error: %w", op, err)
		}
	default:
		if err := h.handleDefault(buf, uctx, src, reqTrace); err != nil {
			return fmt.Errorf("%s: unexpected error: %w", op, err)
		}
	}

	h.log.Info("Request successfully processed",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

// handleDefault processed default content (messages and state)
// via call handleAdmin and handleUser methods
func (h *WSHandler) handleDefault(buf *string, uctx stm.UserContext, msg, reqTrace string) error {
	const op = "delivery.handleDefault"

	h.log.Info("handle default content",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	if err := h.handleAdmin(buf, uctx, msg, reqTrace); err != nil {
		return fmt.Errorf("%s: unexpected error: %w", op, err)
	}
	if *buf == "no handled" {
		if err := h.handleUser(buf, uctx, msg, reqTrace); err != nil {
			return fmt.Errorf("%s: unexpected error: %w", op, err)
		}
	}
	if *buf == "no handled" {
		*buf = "Unknown command or state"
	}

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

// learningTask get task for current user language level
// and updates content in user context
func (h *WSHandler) learningTask(buf *string, uctx stm.UserContext, reqTrace string) error {
	const op = "delivery.learningTask"

	h.log.Info("handle learning task",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

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

	h.log.Info("Get task",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace),
		zap.Int("task_len", len(*buf)))

	if len(*taskBuf) == 0 {
		*buf = "No tasks found"
		return nil
	}

	*buf = (*taskBuf)[0].TaskData
	theme := (*taskBuf)[0].Theme
	answer := (*taskBuf)[0].Answer

	if err := h.usrsrv.UpdateUserTaskCtx(uctx, theme, answer, reqTrace, 0); err != nil {
		return fmt.Errorf("%s: update user task ctx: %w", op, err)
	}

	if err := h.sm.UpdUserStateCtx(stm.StateTaskLearning); err != nil {
		return fmt.Errorf("%s: update state: %w", op, err)
	}

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

// shiritori changes user state to 'StateShiritori'
// and rewrite buffer
func (h *WSHandler) shiritori(buf *string, reqTrace string) error {
	const op = "delivery.shiritori"

	h.log.Info("handle shiritori",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	if err := h.sm.UpdUserStateCtx(stm.StateShiritori); err != nil {
		return fmt.Errorf("%s: change state: %w", op, err)
	}
	*buf = "Shiritori mode activated. Write any word"

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

// chatting changes user state to 'StateAiSetting'
// and rewrite buffer
func (h *WSHandler) chatting(buf *string, reqTrace string) error {
	const op = "delivery.Chatting"

	h.log.Info("handle chatting",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	if err := h.sm.UpdUserStateCtx(stm.StateAiSetting); err != nil {
		return fmt.Errorf("%s: change state: %w", op, err)
	}
	*buf = "Choose your way to use AI:\n" +
		"ttt: text to text\n" +
		"tts: text to speech\n" +
		"stt: speech to text\n" +
		"sts: speech to speech\n"

	h.log.Info("Successfully handled message",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}
