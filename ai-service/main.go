// Package main provides ai service grpc methods.
package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"unsafe"

	"aisrv/internal/rdb"
	"aisrv/internal/router"

	pb "github.com/Votline/EnBooster-Local/protos/generated-ai"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// aiserver is the ai service implementation.
type aiserver struct {
	adminUUID int64
	log       *zap.Logger
	rdb       *rdb.RDB
	rt        *router.Router
	pb.UnimplementedAIServiceServer
}

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 512))
	},
}

var audioPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

var pcmPool = sync.Pool{
	New: func() any {
		b := make([]int16, 0, 4096)
		return &b
	},
}

var userCtxPool = sync.Pool{
	New: func() any {
		b := make([]int, 0, 512)
		return &b
	},
}

const batchSizeThreehold = 128

func main() {
	log, _ := zap.NewDevelopment()
	defer log.Sync()

	lis, err := net.Listen("tcp", ":"+os.Getenv("AISRV_PORT"))
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	rdb, err := rdb.NewRDB()
	if err != nil {
		log.Fatal("failed to connect to database", zap.Error(err))
	}

	rt, err := router.NewRouter()
	if err != nil {
		log.Fatal("failed to create router", zap.Error(err))
	}

	s := aiserver{rdb: rdb, rt: rt, log: log}
	srv := grpc.NewServer()
	pb.RegisterAIServiceServer(srv, &s)

	log.Debug("AI service successfully started")

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatal("failed to serve", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	gracefulShutdown(&s, srv)
}

func gracefulShutdown(s *aiserver, srv *grpc.Server) {
	const op = "aiserver.gracefulShutdown"

	s.log.Info("Shutting down server", zap.String("op", op))

	srv.Stop()
	s.log.Info("Server stopped", zap.String("op", op))

	s.log.Info("Server shutdown successfully", zap.String("op", op))
}

func (s *aiserver) GenerateText(req *pb.GenerateTextReq, stream pb.AIService_GenerateTextServer) error {
	const op = "aiserver.GenerateText"

	var uuid int64 = 1
	prompt := req.GetPrompt()
	sysprompt := req.GetSystemPrompt()
	reqTrace := req.GetRequestTrace()

	s.log.Debug("Stream Generate text request received",
		zap.String("op", op),
		zap.Int("prompt_length", len(prompt)),
		zap.Int("system_prompt_length", len(sysprompt)),
		zap.String("request_trace", reqTrace))

	uctx, err := s.rdb.GetUserContext(uuid)
	if err != nil {
		return fmt.Errorf("%s: : %w", op, err)
	}

	if uctx == nil {
		uctx = make([]int, 0)
	}

	s.log.Debug("User context received",
		zap.String("op", op),
		zap.Int("user_context_length", len(uctx)))

	resBuf := bufPool.Get().(*bytes.Buffer)
	resBuf.Reset()
	defer bufPool.Put(resBuf)

	newUctx := userCtxPool.Get().(*[]int)
	defer userCtxPool.Put(newUctx)

	if err := s.rt.GenerateText(prompt, sysprompt, uctx, newUctx, func(text string) {
		resBuf.WriteString(text)

		if resBuf.Len() > batchSizeThreehold {
			if err := stream.Send(&pb.GenerateTextRes{Text: resBuf.String()}); err != nil {
				s.log.Error("failed to send response", zap.String("op", op), zap.Error(err))
			}
			resBuf.Reset()
		}
	}); err != nil {
		return fmt.Errorf("%s: : %w", op, err)
	}

	if resBuf.Len() > 0 {
		if err := stream.Send(&pb.GenerateTextRes{Text: resBuf.String()}); err != nil {
			s.log.Error("failed to send response", zap.String("op", op), zap.Error(err))
		}
	}

	s.log.Debug("Generate response sent",
		zap.String("op", op),
		zap.Int64("uuid", uuid),
		zap.Int("res_length", resBuf.Len()),
		zap.String("request_trace", reqTrace))

	if err := s.rdb.SetUserContext(uuid, *newUctx); err != nil {
		return fmt.Errorf("%s: : %w", op, err)
	}

	s.log.Debug("User context updated",
		zap.String("op", op),
		zap.Int64("uuid", uuid),
		zap.Int("user_context_length", len(*newUctx)))

	return nil
}

func (s *aiserver) GenerateAudio(ctx context.Context, req *pb.GenerateAudioReq) (*pb.GenerateAudioRes, error) {
	const op = "aiserver.GenerateAudio"

	text := req.GetText()
	reqTrace := req.GetRequestTrace()

	s.log.Debug("Generate audio request received",
		zap.String("op", op),
		zap.Int("text_len", len(text)),
		zap.String("request_trace", reqTrace))

	audioBuf := audioPool.Get().(*[]byte)
	defer audioPool.Put(audioBuf)

	if err := s.rt.GenerateAudio(text, audioBuf, ctx); err != nil {
		return nil, fmt.Errorf("%s: : %w", op, err)
	}

	s.log.Debug("Generate audio response sent",
		zap.String("op", op),
		zap.Int("audio_len", len(*audioBuf)),
		zap.String("request_trace", reqTrace))

	return &pb.GenerateAudioRes{AudioData: *audioBuf}, nil
}

func (s *aiserver) RecognizeAudio(req *pb.RecognizeAudioReq, stream pb.AIService_RecognizeAudioServer) error {
	const op = "aiserver.RecognizeAudio"

	audioData := req.GetAudioData()
	reqTrace := req.GetRequestTrace()

	s.log.Debug("STT message request received",
		zap.String("op", op),
		zap.Int("audio_data_len", len(audioData)),
		zap.String("request_trace", reqTrace))

	pcm := pcmPool.Get().(*[]int16)
	defer pcmPool.Put(pcm)

	if err := decodeOggToPCM(audioData, pcm); err != nil {
		if err.Error() == "too many workers" {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return fmt.Errorf("%s: : %w", op, err)
	}

	if err := s.rt.RecognizeSpeech(*pcm, func(delta string) {
		if err := stream.Send(&pb.RecognizeAudioRes{Text: delta}); err != nil {
			s.log.Error("failed to send response", zap.String("op", op), zap.Error(err))
		}
	}); err != nil {
		return fmt.Errorf("%s: : %w", op, err)
	}

	return nil
}

func (s *aiserver) ClearAIContext(ctx context.Context, req *pb.ClearAIContextReq) (*pb.ClearAIContextRes, error) {
	const op = "aiserver.ClearAIContext"

	var uuid int64 = 1
	reqTrace := req.GetRequestTrace()

	s.log.Debug("Clear AI context request received",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	if err := s.rdb.ClearAIContext(uuid); err != nil {
		return nil, fmt.Errorf("%s: : %w", op, err)
	}

	s.log.Debug("Clear AI context response sent",
		zap.String("op", op),
		zap.String("request_trace", reqTrace))

	return &pb.ClearAIContextRes{}, nil
}

func (s *aiserver) ChangeModel(ctx context.Context, req *pb.ChangeModelReq) (*pb.ChangeModelRes, error) {
	const op = "aiserver.ChangeModel"

	model := req.GetModel()
	reqTrace := req.GetRequestTrace()

	s.log.Debug("Change model req",
		zap.String("op", op),
		zap.String("model", model),
		zap.String("request_trace", reqTrace))

	s.rt.ChangeModel(model)

	s.log.Debug("Successfully changed model",
		zap.String("op", op),
		zap.String("model", model),
		zap.String("request_trace", reqTrace))

	return &pb.ChangeModelRes{}, nil
}

// decodeOggToPCM decodes OGG audio data to PCM via ffmpeg.
func decodeOggToPCM(oggBytes []byte, buf *[]int16) error {
	if len(oggBytes) == 0 {
		return fmt.Errorf("empty audio data input")
	}

	cmd := exec.Command("ffmpeg",
		"-hide_banner",
		"-loglevel", "error",
		"-i", "pipe:0",
		"-f", "s16le",
		"-ac", "1",
		"-ar", os.Getenv("STT_SAMPLE_RATE"),
		"pipe:1",
	)

	cmd.Stdin = bytes.NewReader(oggBytes)
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg error: %w (stderr: %s)", err, errBuf.String())
	}

	pcmBytes := outBuf.Bytes()
	if len(pcmBytes) == 0 {
		return fmt.Errorf("ffmpeg generated empty PCM stream")
	}

	if len(pcmBytes)%2 != 0 {
		pcmBytes = pcmBytes[:len(pcmBytes)-1]
	}

	pcmI16 := unsafe.Slice((*int16)(unsafe.Pointer(&pcmBytes[0])), len(pcmBytes)/2)
	*buf = pcmI16

	return nil
}
