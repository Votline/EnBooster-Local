// Package users user.go implement Service interface
// and make connect to users-service by gRPC
package users

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"enbstr/internal/cbreaker"
	stm "enbstr/internal/statemanager"
	"enbstr/internal/ui"

	pb "github.com/Votline/EnBooster/protos/generated-users"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	tele "gopkg.in/telebot.v3"
)

// UsersService is a struct that implements Service interface
// and makes connect to users-service by gRPC
type UsersService struct {
	name        string
	adminUUID   int64
	langLevels  map[string]struct{}
	notifyMsgs  []string
	uiInstns    *ui.UI
	sm          *stm.StateManager
	ctxTimeout  time.Duration
	log         *zap.Logger
	cb          *gobreaker.CircuitBreaker[any]
	conn        *grpc.ClientConn
	client      pb.UsersServiceClient
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
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

// NewUS creates new UsersService instance
func NewUS(ctxTimeout time.Duration, adminUUID int64, uiInstns *ui.UI, sm *stm.StateManager, bot *tele.Bot, log *zap.Logger) (*UsersService, error) {
	const op = "users.NewUS"

	log.Info("Creating users service",
		zap.String("op", op))

	rpcCert, err := getTLSConfig(os.Getenv("TLS_SERVER_NAME"), "ssl/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("%s: get certs: %w", op, err)
	}

	conn, err := grpc.NewClient(
		os.Getenv("USERS_HOST")+":"+os.Getenv("USERS_PORT"),
		grpc.WithTransportCredentials(credentials.NewTLS(rpcCert)))
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create client: %w", op, err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(os.Getenv("KAFKA_ADDR")),
		Topic:    os.Getenv("KAFKA_TOPIC_GTW_US"),
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_ADDR")},
		Topic:    os.Getenv("KAFKA_TOPIC_US_GTW"),
		GroupID:  os.Getenv("KAFKA_GROUP_ID"),
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	langLevels := make(map[string]struct{})
	langLevelsStr := os.Getenv("LANG_LEVELS")
	langLevelsSlc := strings.Split(langLevelsStr, " ")
	for _, lvl := range langLevelsSlc {
		if lvl == "" {
			continue
		}
		langLevels[lvl] = struct{}{}
	}

	notifyMsgs := strings.Split(os.Getenv("NOTIFICATION_MESSAGES"), ";")

	srv := &UsersService{
		name:        "users",
		adminUUID:   adminUUID,
		langLevels:  langLevels,
		notifyMsgs:  notifyMsgs,
		uiInstns:    uiInstns,
		sm:          sm,
		ctxTimeout:  ctxTimeout,
		log:         log,
		conn:        conn,
		cb:          cbreaker.NewCB("users", log),
		client:      pb.NewUsersServiceClient(conn),
		kafkaWriter: writer,
		kafkaReader: reader,
	}

	srv.registerRoutes(bot)

	return srv, nil
}

func (us *UsersService) registerRoutes(bot *tele.Bot) {
	const op = "users.registerRoutes"

	bot.Handle(ui.AISystemPromptID, func(c tele.Context) error {
		c.Respond()
		us.log.Debug("Catch change ai system prompt event",
			zap.String("op", op))

		if err := c.Send("Write new system prompt. For default send 'default'", us.uiInstns.UserMain); err != nil {
			return fmt.Errorf("%s: bot send: %w", op, err)
		}

		if err := us.HandleRoutes(ui.AISystemPromptID, c); err != nil {
			us.log.Error("Handle change system prompt button",
				zap.String("op", op),
				zap.Error(err))
			return fmt.Errorf("%s: Handle change system prompt: %w", op, err)
		}
		us.log.Debug("Successfully changed state",
			zap.String("op", op))
		return nil
	})

	bot.Handle(ui.ChangeLangLevelID, func(c tele.Context) error {
		c.Respond()
		us.log.Debug("Catch change language level event",
			zap.String("op", op))

		if err := c.Send("Write new language level", us.uiInstns.UserMain); err != nil {
			return fmt.Errorf("%s: bot send: %w", op, err)
		}

		if err := us.HandleRoutes(ui.ChangeLangLevelID, c); err != nil {
			us.log.Error("Handle change language level button",
				zap.String("op", op),
				zap.Error(err))
			return fmt.Errorf("%s: Handle change language level: %w", op, err)
		}
		us.log.Debug("Successfully changed state",
			zap.String("op", op))
		return nil
	})
}

// HandleRoutes handle user messages which intended for user-service
func (us *UsersService) HandleRoutes(msg string, c tele.Context) error {
	const op = "users.RegisterRoutes"

	userID := c.Message().Sender.ID
	chatID := c.Message().Chat.ID
	var menu *tele.ReplyMarkup
	if userID == us.adminUUID {
		menu = us.uiInstns.AdminMain
	} else {
		menu = us.uiInstns.UserMain
	}

	reqTrace := uuid.NewString()

	switch msg {
	case "/start":
		us.log.Debug("Catch start command",
			zap.Int64("user_id", c.Message().Sender.ID),
			zap.String("op", op))

		if err := c.Send("Welcome to EnBooster!", menu); err != nil {
			return fmt.Errorf("%s: send welcome message: %w", op, err)
		}

		if err := us.Register(userID, chatID, reqTrace); err != nil {
			return fmt.Errorf("%s: register user: %w", op, err)
		}
	case "Profile":
		uuid := c.Message().Sender.ID
		ud, err := us.GetData(uuid, reqTrace)
		if err != nil {
			return fmt.Errorf("%s: get user data: %w", op, err)
		}

		if err := c.Send(fmt.Sprintf("Your data:\nUUID: %d\nLevel: %s\nTask position:%d\nBest theme: %s | %d\nWorst theme: %s | %d\nStreak: %d\nSystem prompt: %s",
			ud.UUID, ud.Level, ud.TaskID,
			ud.BestTheme, ud.BestThemeCnt,
			ud.WorstTheme, ud.WorstThemeCnt,
			ud.Streak, ud.SystemPrompt), menu, us.uiInstns.UserSettings); err != nil {
			return fmt.Errorf("%s: send user data: %w", op, err)
		}
	case ui.AISystemPromptID:
		if err := us.sm.UpdUserStateCtx(c.Sender().ID, stm.StateSetSysPrompt); err != nil {
			return fmt.Errorf("%s: update user state: %w", op, err)
		}
		return nil
	case ui.ChangeLangLevelID:
		if err := us.sm.UpdUserStateCtx(c.Sender().ID, stm.StateSetLangLevel); err != nil {
			return fmt.Errorf("%s: update user state: %w", op, err)
		}
		return nil
	}

	return nil
}

// Close closes the connection to the server
func (us *UsersService) Close() error {
	const op = "users.Close"

	errStr := ""
	if err := us.kafkaWriter.Close(); err != nil {
		errStr += err.Error()
	}
	if err := us.conn.Close(); err != nil {
		errStr += err.Error()
	}
	if errStr != "" {
		return fmt.Errorf("%s: %s", op, errStr)
	}

	return nil
}

// GetName returns the name of the service
func (us *UsersService) GetName() string {
	return us.name
}
