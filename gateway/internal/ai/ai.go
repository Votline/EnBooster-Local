// Package ai ai.go implement Service interface
// and make connect to ai-service by gRPC
package ai

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"time"
	"unsafe"

	"enbstr/internal/cbreaker"
	stm "enbstr/internal/statemanager"
	"enbstr/internal/ui"

	pb "github.com/Votline/EnBooster/protos/generated-ai"
	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	tele "gopkg.in/telebot.v3"
)

// AIService is a struct that implements Service interface
// and makes connect to ai-service by gRPC
type AIService struct {
	name       string
	uiInstns   *ui.UI
	ctxTimeout time.Duration
	sm         *stm.StateManager
	log        *zap.Logger
	cb         *gobreaker.CircuitBreaker[any]
	conn       *grpc.ClientConn
	client     pb.AIServiceClient
}

// getTLSConfig returns tls config from path with servername
func getTLSConfig(srvName, path string) (*tls.Config, error) {
	caCert, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("get certs: %w", err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)

	config := &tls.Config{
		RootCAs:    certPool,
		ServerName: srvName,
	}

	return config, nil
}

// NewAIS creates new AIService instance
func NewAIS(ctxTimeout time.Duration, sm *stm.StateManager, uiInstns *ui.UI, bot *tele.Bot, log *zap.Logger) (*AIService, error) {
	const op = "ai.NewAIS"

	log.Info("Creating ai service",
		zap.String("op", op))

	rpcCert, err := getTLSConfig(os.Getenv("TLS_SERVER_NAME"), "ssl/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("%s: get certs: %w", op, err)
	}

	conn, err := grpc.NewClient(
		os.Getenv("AISRV_HOST")+":"+os.Getenv("AISRV_PORT"),
		grpc.WithTransportCredentials(credentials.NewTLS(rpcCert)))
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create client: %w", op, err)
	}

	srv := &AIService{
		name:       "ai",
		uiInstns:   uiInstns,
		ctxTimeout: ctxTimeout,
		sm:         sm,
		log:        log,
		conn:       conn,
		cb:         cbreaker.NewCB("ai", log),
		client:     pb.NewAIServiceClient(conn),
	}

	srv.registerRoutes(bot)

	return srv, nil
}

// registerRoutes handles events from Telegram.
func (ai *AIService) registerRoutes(bot *tele.Bot) {
	const op = "ai.registerRoutes"

	bot.Handle(ui.TranscriptVoiceID, func(c tele.Context) error {
		c.Respond()
		ai.log.Debug("Catch transcript voice event",
			zap.String("op", op))

		if err := ai.HandleRoutes(ui.TranscriptVoiceID, c); err != nil {
			ai.log.Error("Handle change system prompt button",
				zap.String("op", op),
				zap.Error(err))
			c.Send("Something went wrong. Try again later")
			return fmt.Errorf("%s: Handle change system prompt: %w", op, err)
		}
		ai.log.Debug("Successfully changed state",
			zap.String("op", op))
		return nil
	})

	bot.Handle(ui.ClearAIContextID, func(c tele.Context) error {
		c.Respond()
		reqTrace := uuid.NewString()
		ai.log.Debug("Catch clear ai context event",
			zap.String("op", op))

		if err := ai.ClearAIContext(c.Sender().ID, reqTrace); err != nil {
			ai.log.Error("Handle clear ai context button",
				zap.String("op", op),
				zap.Error(err))
			c.Send("Something went wrong. Try again later")
			return fmt.Errorf("%s: Handle clear ai context: %w", op, err)
		}
		ai.log.Debug("Successfully cleared ai context",
			zap.String("op", op))

		if err := c.Send("AI context cleared."+
			" The context is cleared every 10 minutes",
			ai.uiInstns.UserMain); err != nil {
			return fmt.Errorf("%s: bot send: %w", op, err)
		}
		return nil
	})
}

// HandleRoutes handle user messages which intended for ai-service
func (ai *AIService) HandleRoutes(msg string, c tele.Context) error {
	const op = "ai.RegisterRoutes"

	reqTrace := uuid.NewString()

	switch msg {
	case ui.TranscriptVoiceID:
		usrctx, err := ai.sm.GetUserCtx(c.Sender().ID)
		if err != nil {
			return fmt.Errorf("%s: get user state: %w", op, err)
		}

		ai.log.Debug("Transcript voice",
			zap.String("op", op),
			zap.String("request_trace", reqTrace))

		var aiSes stm.ChattingSession
		if len(usrctx.JSONData) > 0 {
			bytesData := unsafe.Slice(unsafe.StringData(usrctx.JSONData), len(usrctx.JSONData))
			if err := json.Unmarshal(bytesData, &aiSes); err != nil {
				return fmt.Errorf("%s: unmarshal: %w", op, err)
			}
		}

		if aiSes.LastMessage == "" {
			return fmt.Errorf("%s: empty last message", op)
		}

		if err := c.Send(aiSes.LastMessage, ai.uiInstns.UserMain); err != nil {
			return fmt.Errorf("%s: bot send: %w", op, err)
		}
	case "Chatting":
		if err := c.Send("Choose your way to use AI", ai.uiInstns.AISettings); err != nil {
			return fmt.Errorf("%s: bot send: %w", op, err)
		}
		if err := ai.sm.UpdUserStateCtx(c.Sender().ID, stm.StateAiSetting); err != nil {
			return fmt.Errorf("%s: upd user state: %w", op, err)
		}
	}

	return nil
}

// Close closes the connection to the server
func (ai *AIService) Close() error {
	return ai.conn.Close()
}

// GetName returns the name of the service
func (ai *AIService) GetName() string {
	return ai.name
}
