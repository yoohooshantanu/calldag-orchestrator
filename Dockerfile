# Multi-stage Dockerfile for AudioPipeline
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /audio-pipeline ./cmd/server

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /audio-pipeline /app/audio-pipeline

EXPOSE 8081
ENTRYPOINT ["/app/audio-pipeline"]
