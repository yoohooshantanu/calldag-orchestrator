# Multi-stage Dockerfile for VoicePipe
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /voicepipe ./cmd/server

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /voicepipe /app/voicepipe

EXPOSE 8081
ENTRYPOINT ["/app/voicepipe"]
