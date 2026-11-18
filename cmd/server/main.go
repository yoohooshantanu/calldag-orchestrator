package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/audio-pipeline/internal/broker"
	"github.com/yoohooshantanu/audio-pipeline/internal/logger"
	"github.com/yoohooshantanu/audio-pipeline/internal/metrics"
	"github.com/yoohooshantanu/audio-pipeline/internal/model"
	"github.com/yoohooshantanu/audio-pipeline/internal/orchestrator"
	"github.com/yoohooshantanu/audio-pipeline/internal/stream"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8081")
	redisAddr := os.Getenv("REDIS_ADDR")
	workersStr := getEnv("WORKER_CONCURRENCY", "8")
	numWorkers, _ := strconv.Atoi(workersStr)

	logger.L.Info("starting_audio-pipeline", "port", port, "workers", numWorkers)

	// -------
	// Message broker initialization
	// -------
	var msgBroker broker.MessageBroker
	if redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		msgBroker = broker.NewRedisStreamBroker(rdb)
		logger.L.Info("broker_redis_connected", "addr", redisAddr)
	} else {
		msgBroker = broker.NewMemoryBroker(2048)
		logger.L.Info("broker_memory_mode")
	}

	streamMgr := stream.NewStreamManager(5 * time.Minute)

	// -------
	// Workflow state store and orchestrator worker pool
	// -------
	stateStore := orchestrator.NewMemoryStateStore()
	orch := orchestrator.NewWorkflowOrchestrator(
		context.Background(),
		msgBroker,
		stateStore,
		"audio_pipeline_pipeline_jobs",
		numWorkers,
	)
	orch.Start()

	// -------
	// HTTP Gateway and streaming API endpoints
	// -------
	mux := http.NewServeMux()

	// -------
	// Audio stream chunk ingestion
	// -------
	mux.HandleFunc("/v1/streams/audio", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		callID := r.Header.Get("X-Call-ID")
		seqStr := r.Header.Get("X-Sequence")
		if callID == "" || seqStr == "" {
			http.Error(w, `{"error":"X-Call-ID and X-Sequence headers required"}`, http.StatusBadRequest)
			return
		}

		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid X-Sequence header"}`, http.StatusBadRequest)
			return
		}

		audioData, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"cannot read audio body"}`, http.StatusBadRequest)
			return
		}

		isFinal := r.Header.Get("X-Stream-Final") == "true"
		chunk := &model.AudioChunk{
			CallID:    callID,
			Sequence:  seq,
			Timestamp: time.Now().UnixMilli(),
			Data:      audioData,
			Codec:     "pcm_s16le",
			IsFinal:   isFinal,
		}

		buf := streamMgr.GetOrCreate(callID)
		ack, err := buf.IngestChunk(chunk)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(ack)
			return
		}

		_ = msgBroker.PublishStream(r.Context(), "audio:"+callID, chunk)

		// -------
		// Broadcast stream completion metadata on final chunk
		// -------
		if isFinal {
			assembled, lastSeq, totalFrames := buf.AssembleContiguousAudio()
			_ = msgBroker.PublishStream(r.Context(), "audio:completed:"+callID, map[string]interface{}{
				"call_id":      callID,
				"total_frames": totalFrames,
				"last_seq":     lastSeq,
				"total_bytes":  len(assembled),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(ack)
	})

	// -------
	// Pipeline job lifecycle management
	// -------
	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var req struct {
				CallID       string                 `json:"call_id"`
				TenantID     string                 `json:"tenant_id"`
				WorkflowType string                 `json:"workflow_type"`
				Payload      map[string]interface{} `json:"payload"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
				return
			}

			if req.CallID == "" {
				http.Error(w, `{"error":"call_id is required"}`, http.StatusBadRequest)
				return
			}
			if req.WorkflowType == "" {
				req.WorkflowType = "post_call_ai_pipeline"
			}

			job, err := orch.SubmitJob(r.Context(), req.CallID, req.TenantID, req.WorkflowType, req.Payload)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(job)

		} else if r.Method == http.MethodGet {
			callID := r.URL.Query().Get("call_id")
			if callID == "" {
				http.Error(w, `{"error":"call_id query parameter required"}`, http.StatusBadRequest)
				return
			}

			jobs, err := stateStore.ListJobsByCall(r.Context(), callID)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(jobs)
		} else {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/v1/jobs/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 3 || parts[1] != "jobs" {
			http.Error(w, `{"error":"invalid job URL format"}`, http.StatusBadRequest)
			return
		}
		jobID := parts[2]

		job, err := stateStore.GetJob(r.Context(), jobID)
		if err != nil {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(job)
	})

	// -------
	// Telemetry and observability endpoints
	// -------
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":         "healthy",
			"active_streams": streamMgr.ActiveCount(),
			"timestamp":      time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/metrics", metrics.DefaultMetrics.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.L.Info("audio_pipeline_listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("server_listen_failed", "error", err)
		}
	}()

	// -------
	// Graceful shutdown on termination signal
	// -------
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_audio-pipeline")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = server.Shutdown(ctx)
	orch.Stop()
	streamMgr.Stop()
	_ = msgBroker.Close()
	logger.L.Info("audio_pipeline_terminated_cleanly")
}
