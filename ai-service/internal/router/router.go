// Package router makes requests to local AI
package router

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"aisrv/internal/utils"

	vosk "github.com/alphacep/vosk-api/go"
)

// option contains AI model options.
type option struct {
	Temperature   float64 `json:"temperature"`
	NumPredicts   int     `json:"num_predict"`
	TopP          float64 `json:"top_p"`
	RepeatPenalty float64 `json:"repeat_penalty"`
}

// requestBody used for HTTP request body to AI.
type requestBody struct {
	Model   string `json:"model"`
	System  string `json:"system"`
	Prompt  string `json:"prompt"`
	Stream  bool   `json:"stream"`
	Context []int  `json:"context"`
	Options option `json:"options"`
}

// aiResponse used for decode response from AI
type aiResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
	Context  []int  `json:"context"`
}

// textToText used for text to text.
type textToText struct {
	url      string
	defaulsp string
	client   *http.Client
	reqBody  requestBody
}

// textToSpeech used for text to speech.
type textToSpeech struct {
	path string
	args []string
}

// speechToText used for speech recognition.
type speechToText struct {
	rec      *vosk.VoskRecognizer
	start    int
	estEnd   int
	end      int
	skipCnt  int
	trashLen int
	defLen   int
}

// Router contains all needed fields to call AI
type Router struct {
	timeout int
	ttt     textToText
	tts     textToSpeech
	stt     speechToText
}

const (
	sttTrashLen    = len(`"partial": "`)
	sttDefLen      = 256
	sttStep        = 3200
	patternFinal   = `"text" : "`
	patternPartial = `"partial" : "`
)

func NewRouter() (*Router, error) {
	const op = "router.NewRouter"

	model := os.Getenv("AI_MODEL")
	system := os.Getenv("AI_SYSTEM")
	timeout := utils.GetEnvInt(os.Getenv("AI_TIMEOUT"), 60) * int(time.Second)

	url := fmt.Sprintf("http://%s:%s/api/generate", os.Getenv("AI_HOST"), os.Getenv("AI_PORT"))
	defaultSystemPrompt := os.Getenv("AI_DEFAULT_SYSTEM_PROMPT")

	temp := utils.GetEnvFloat(os.Getenv("AI_TEMP"), 0.7)
	numPredicts := utils.GetEnvInt(os.Getenv("AI_NUM_PREDICTS"), 512)
	topP := utils.GetEnvFloat(os.Getenv("AI_TOP_P"), 0.95)
	repeatPenalty := utils.GetEnvFloat(os.Getenv("AI_REPEAT_PENALTY"), 1.1)

	pathTTS := os.Getenv("TTS_PATH")
	modelTTS := os.Getenv("TTS_MODEL")
	sampleRate := utils.GetEnvInt(os.Getenv("TTS_SAMPLE_RATE"), 16000)
	args := []string{
		"-p", modelTTS,
		"-R", strconv.Itoa(sampleRate),
		"-o", "/dev/stdout",
	}

	sttPath := os.Getenv("STT_PATH")
	sttSampleRate := utils.GetEnvInt(os.Getenv("STT_SAMPLE_RATE"), 16000)

	sttModel, err := vosk.NewModel(sttPath)
	if err != nil {
		return nil, fmt.Errorf("%s: vosk.NewModel: %w", op, err)
	}
	rec, err := vosk.NewRecognizer(sttModel, float64(sttSampleRate))
	if err != nil {
		return nil, fmt.Errorf("%s: vosk.NewRecognizer: %w", op, err)
	}
	rec.SetMaxAlternatives(0)

	return &Router{
		ttt: textToText{
			reqBody: requestBody{
				Model:  model,
				System: system,
				Stream: true,
				Options: option{
					Temperature:   temp,
					NumPredicts:   numPredicts,
					TopP:          topP,
					RepeatPenalty: repeatPenalty,
				},
			},
			url:      url,
			client:   http.DefaultClient,
			defaulsp: defaultSystemPrompt,
		},
		tts: textToSpeech{
			path: pathTTS,
			args: args,
		},
		stt: speechToText{
			rec:   rec,
			start: 0,
			end:   sttDefLen - sttTrashLen,
		},
		timeout: timeout,
	}, nil
}

// GenerateText generates text from AI.
func (r Router) GenerateText(prompt, systemPrompt string, userContext []int, buf *[]int, yield func(string)) error {
	const op = "router.GenerateText"

	r.ttt.reqBody.Prompt = prompt
	r.ttt.reqBody.Context = userContext
	r.ttt.reqBody.Stream = true

	r.ttt.reqBody.System = systemPrompt
	switch systemPrompt {
	case "default":
		r.ttt.reqBody.System = r.ttt.defaulsp
	case "nop":
		r.ttt.reqBody.System = ""
	}

	jsonData, err := json.Marshal(r.ttt.reqBody)
	if err != nil {
		return fmt.Errorf("%s:json.Marshal: %w", op, err)
	}
	jsonReader := bytes.NewReader(jsonData)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.timeout)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.ttt.url, jsonReader)
	if err != nil {
		return fmt.Errorf("%s: http.NewRequestWithContext: %w", op, err)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := r.ttt.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: client.Do: %w", op, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: bad status code: %d", op, res.StatusCode)
	}

	var lastContext []int
	reader := bufio.NewReader(res.Body)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%s: reader.ttt.ReadBytes: %w", op, err)
		}
		aiRes := aiResponse{}
		if err := json.Unmarshal(line, &aiRes); err != nil {
			return fmt.Errorf("%s: json.Unmarshal: %w", op, err)
		}
		lastContext = aiRes.Context
		yield(aiRes.Response)
	}

	*buf = lastContext

	return nil
}

// GenerateAudio calls script to generate audio from text.
func (r Router) GenerateAudio(text string, buf *[]byte, ctx context.Context) error {
	const op = "router.GenerateAudio"

	cmd := exec.CommandContext(ctx, r.tts.path, r.tts.args...)

	cmd.Stdin = strings.NewReader(text)

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%s: context done: %w", op, ctx.Err())
		}
		return fmt.Errorf("%s: cmd.Run: %w, stderr: %s", op, err, errBuf.String())
	}

	*buf = outBuf.Bytes()

	return nil
}

// RecognizeSpeech uses Vosk to recognize speech from PCM stream.
func (r Router) RecognizeSpeech(pcmI16 []int16, yield func(delta string)) error {
	const op = "router.RecognizeSpeech"

	if len(pcmI16) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.timeout)*time.Second)
	defer cancel()

	for i := 0; i < len(pcmI16); i += sttStep {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: context done: %w", op, ctx.Err())
		default:
		}

		endIdx := i + sttStep
		endIdx = min(endIdx, len(pcmI16))

		chunk := pcmI16[i:endIdx]

		r.processChunk(chunk, func(delta []byte) {
			deltaStr := unsafe.String(unsafe.SliceData(delta), len(delta))
			yield(deltaStr)
		})
	}

	finalJSON := r.stt.rec.FinalResult()
	var trimmed []byte
	if len(finalJSON) > 0 {
		trimmed = []byte(finalJSON)
		trimJSON(&trimmed, patternFinal)
		if len(trimmed) > 0 && r.stt.start < len(trimmed) {
			yield(string(trimmed[r.stt.start:]))
		}
	}

	return nil
}

// processChunk uses window method to process chunk of PCM stream.
// It makes a custom data stream with result from Vosk
func (r Router) processChunk(chunkI16 []int16, yield func(delta []byte)) {
	if len(chunkI16) == 0 {
		return
	}

	bytesSamples := unsafe.Slice((*byte)(unsafe.Pointer(&chunkI16[0])), len(chunkI16)*2)

	final := r.stt.rec.AcceptWaveform(bytesSamples)

	var res string
	jsonPattern := ""

	if final == 1 {
		res = r.stt.rec.Result()
		jsonPattern = patternFinal
	} else {
		res = r.stt.rec.PartialResult()
		jsonPattern = patternPartial
	}

	if len(res) == 0 {
		return
	}

	trimmed := unsafe.Slice(unsafe.StringData(res), len(res))
	trimJSON(&trimmed, jsonPattern)

	if len(trimmed) == 0 {
		return
	}

	if final == 1 {
		if r.stt.start < len(trimmed) {
			yield(trimmed[r.stt.start:])
		}
		r.stt.start = 0
		r.stt.skipCnt = 0
		return
	}

	if len(trimmed) < r.stt.start {
		return
	}

	if r.stt.skipCnt >= 10 {
		r.stt.skipCnt = 0

		localEnd := bytes.IndexByte(trimmed[r.stt.estEnd:], ' ')
		if localEnd == -1 {
			localEnd = r.stt.estEnd
		} else {
			localEnd += r.stt.estEnd
		}
		r.stt.end = localEnd

		if r.stt.start < r.stt.end {
			yield(trimmed[r.stt.start:r.stt.end])
		}

		r.stt.start = r.stt.end
		r.stt.estEnd = 0
		r.stt.end = sttDefLen - sttTrashLen
	} else if r.stt.skipCnt == 0 {
		r.stt.skipCnt++
		r.stt.estEnd = r.stt.end
	} else {
		r.stt.skipCnt++
	}
}

// float32ToVosk converts float32 slice to bytes
func float32ToVosk(pcm []float32, int16Samples []int16) {
	if len(pcm) == 0 {
		return
	}

	for i, f := range pcm {
		if f > 1.0 {
			f = 1.0
		} else if f < -1.0 {
			f = -1.0
		}

		int16Samples[i] = int16(f * 32767.0)
	}
}

// trimJSON removes the pattern from the JSON
// Used for removing "partial" : "" from the JSON
func trimJSON(d *[]byte, patternStr string) {
	json := *d

	patternBytes := unsafe.Slice(unsafe.StringData(patternStr), len(patternStr))

	start := bytes.Index(json, patternBytes)
	if start == -1 {
		return
	}
	start += len(patternBytes)

	end := bytes.LastIndexByte(json[start:], '"')
	if end == -1 {
		*d = json[start:]
		return
	}
	end += start

	*d = json[start:end]
}
