FROM golang:1.26-bookworm AS builder

WORKDIR /build/aiservice

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl unzip && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/include /usr/lib && \
    curl -L https://github.com/alphacep/vosk-api/releases/download/v0.3.45/vosk-linux-x86_64-0.3.45.zip -o vosk.zip && \
    unzip vosk.zip && \
    cp vosk-linux-x86_64-0.3.45/vosk_api.h /usr/include/ && \
    cp vosk-linux-x86_64-0.3.45/libvosk.so /usr/lib/ && \
    rm -rf vosk.zip vosk-linux-x86_64-0.3.45

COPY ai-service/go.mod ai-service/go.sum ./
RUN go mod download -x

COPY ai-service ./
RUN CGO_ENABLED=1 go build -o aiservice main.go

FROM archlinux:latest

WORKDIR /aiservice

RUN pacman -Syu --noconfirm \
    rhvoice \
    rhvoice-language-english \
    rhvoice-voice-slt \
    vosk-api \
    bash \
    ffmpeg \
    && pacman -Scc --noconfirm

COPY --from=builder /build/aiservice/aiservice ./
COPY --from=builder /build/aiservice/ssl ./ssl

COPY ai-service/scripts ./scripts
RUN chmod +x ./scripts/rhvoice

CMD ["./aiservice"]
