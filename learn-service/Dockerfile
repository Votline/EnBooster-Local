FROM golang:1.26.3-alpine AS builder

WORKDIR /build/learnservice

COPY learn-service/go.mod learn-service/go.sum ./
RUN go mod download -x

COPY learn-service ./
RUN go build -o learnservice main.go

FROM alpine:3.18

WORKDIR /learnservice

COPY --from=builder /build/learnservice/learnservice ./
CMD ["./learnservice"]
