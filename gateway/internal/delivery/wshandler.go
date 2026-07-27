// Package delivery make connect with frontend
// catch messages and send bot answer
package delivery

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WSHandler main struct for BFF connection
type WSHandler struct {
	log      *zap.Logger
	upgrader *websocket.Upgrader
}

// chatMsg used for send/accept message from frontend
type chatMsg struct {
	UUID  string `json:"uuid"`
	Text  string `json:"text"`
	IsBot bool   `json:"is_bot"`
}

// NewWSH creates new WSHandler instance
func NewWSH(log *zap.Logger) *WSHandler {
	return &WSHandler{
		log: log,
		upgrader: &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// RegisterRoutes register routes for WSHandler
func (h *WSHandler) RegisterRoutes(s *http.ServeMux) {
	s.HandleFunc("/ws", h.HandleChatWS)
}

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

		reply := chatMsg{
			UUID:  uuid.NewString(),
			Text:  "Bot accepted: " + msg.Text,
			IsBot: true,
		}

		if err := conn.WriteJSON(reply); err != nil {
			h.log.Error("WS write failed",
				zap.String("op", op),
				zap.Error(err))
			break
		}
	}
}
