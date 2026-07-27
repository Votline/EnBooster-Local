FROM golang:1.26.3-alpine AS builder

WORKDIR /build/usersservice

COPY users-service/go.mod users-service/go.sum ./
RUN go mod download -x

COPY users-service ./
RUN go build -o usersservice main.go

FROM alpine:3.18

WORKDIR /usersservice

COPY --from=builder /build/usersservice/usersservice ./
COPY --from=builder /build/usersservice/ssl ./ssl
CMD ["./usersservice"]
