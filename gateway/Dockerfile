FROM golang:1.26.3-alpine AS builder

WORKDIR /build/gateway

COPY gateway/go.mod gateway/go.sum ./
RUN go mod download -x

COPY gateway ./
RUN go build -o gateway main.go

FROM alpine:3.18

WORKDIR /gateway

COPY --from=builder /build/gateway/gateway ./
CMD ["./gateway"]
