// Package ai handler.go contains methods for
// call ai service grpc methods
package ai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"unsafe"

	pb "github.com/Votline/EnBooster-Local/protos/generated-ai"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GenerateText call ai-service Generate method
func (ai *AIService) GenerateText(prompt, sysPrompt, reqTrace string, yield func(res []byte)) error {
	const op = "ai.GenerateText"

	ai.log.Debug("Generate text request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace),
		zap.Int("prompt_len", len(prompt)))

	ctx, cancel := context.WithTimeout(context.Background(), ai.ctxTimeout)
	defer cancel()

	stream, err := ai.client.GenerateText(ctx, &pb.GenerateTextReq{
		Prompt:       prompt,
		SystemPrompt: sysPrompt,
		RequestTrace: reqTrace,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
			return fmt.Errorf("ResourceExhausted")
		}
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ai.log.Debug("Successfully connected to ai-service stream",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	totalLen := 0
	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%s: stream recv: %w", op, err)
		}

		ai.log.Debug("Received response from ai-service",
			zap.String("op", op),
			zap.String("reqTrace", reqTrace))

		yield(unsafe.Slice(unsafe.StringData(res.Text), len(res.Text)))
		totalLen += len(res.Text)
	}

	ai.log.Debug("Generate text successfully",
		zap.String("op", op),
		zap.Int("totalLen", totalLen),
		zap.String("reqTrace", reqTrace))

	return nil
}

func (ai *AIService) GenerateAudio(usrMsg, reqTrace string, yield func(res []byte)) error {
	const op = "ai.GenerateAudio"

	ai.log.Debug("Generate audio request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace),
		zap.Int("prompt_len", len(usrMsg)))

	res, err := ai.client.GenerateAudio(context.Background(), &pb.GenerateAudioReq{
		Text:         usrMsg,
		RequestTrace: reqTrace,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
			return fmt.Errorf("ResourceExhausted")
		}
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ai.log.Debug("Generate audio successfully",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	yield(res.AudioData)

	return nil
}

func (ai *AIService) RecognizeAudio(oggBytes []byte, reqTrace string, yield func(res string)) error {
	const op = "ai.RecognizeAudio"

	ai.log.Debug("Recognize audio request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	stream, err := ai.client.RecognizeAudio(context.Background(), &pb.RecognizeAudioReq{
		AudioData:    oggBytes,
		RequestTrace: reqTrace,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
			return fmt.Errorf("ResourceExhausted")
		}
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ai.log.Debug("Successfully connected to ai-service",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%s: stream recv: %w", op, err)
		}

		ai.log.Debug("Received response from ai-service",
			zap.String("op", op),
			zap.String("reqTrace", reqTrace))

		yield(res.Text)
	}

	ai.log.Debug("Recognize audio successfully",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}

func (ai *AIService) ClearAIContext(reqTrace string) error {
	const op = "ai.ClearAIContext"

	ai.log.Debug("Clear ai context request",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	if _, err := ai.client.ClearAIContext(context.Background(), &pb.ClearAIContextReq{
		RequestTrace: reqTrace,
	}); err != nil {
		return fmt.Errorf("%s: rpc call: %w", op, err)
	}

	ai.log.Debug("Clear ai context successfully",
		zap.String("op", op),
		zap.String("reqTrace", reqTrace))

	return nil
}
