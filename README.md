# AudioPipeline: Distributed Real-Time Audio Chunk Pipeline & Async Workflow Orchestrator

[![CI Pipeline](https://github.com/yoohooshantanu/audio-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/yoohooshantanu/audio-pipeline/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**AudioPipeline** is a high-throughput, low-latency audio chunk streaming and distributed asynchronous workflow orchestrator written in Go. It is purpose-built to solve the dual challenges of real-time conversational AI voice systems:

1. **Sub-millisecond Real-Time Fast Path**: Zero-loss buffering, out-of-order reassembly, and flow control for streaming telephony audio chunks (PCM/Opus).
2. **Resilient Long-Running Async Workflow Path**: Distributed multi-stage task orchestration (post-call audio stitching, speech transcription, LLM conversation summarization, and CRM synchronization) with worker leasing, automatic exponential retries, and Dead Letter Queue (DLQ) state persistence.

---

## 1. Architecture Overview

```
Telephony Audio Stream (SIP / WebRTC / Gateway)
            │
            │ POST /v1/streams/audio [X-Call-ID, X-Sequence, Binary PCM Payload]
            ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 1. Real-Time Audio Buffer Manager (< 1 µs Ingest)                      │
│    ├── Monotonic Sequence Frontier Tracker                             │
│    ├── Out-of-order frame reassembly in contiguous memory               │
│    └── Rejection of backwards duplicate frames                         │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  ▼                                   ▼
┌──────────────────────────────────┐ ┌───────────────────────────────────┐
│ 2. Distributed Stream Pub/Sub    │ │ 3. Asynchronous DAG Orchestrator │
│    ├── Redis Streams / In-Memory │ │    ├── Multi-Step Pipeline:       │
│    ├── Broadcast to live ASR     │ │        Step 1: Audio Stitching    │
│    └── Zero-blocking drop for    │ │        Step 2: Speech ASR         │
│        slow listeners            │ │        Step 3: LLM Summary        │
│                                  │ │        Step 4: CRM / Webhook Push │
│                                  │ │    ├── Worker Heartbeats & Leases │
│                                  │ │    └── Auto-Retry + DLQ Routing   │
└──────────────────────────────────┘ └───────────────────────────────────┘
```

---

## 2. Key Engineering Pillars

### A. Sub-Millisecond Audio Frame Reassembly
Audio frames arriving over network connections frequently arrive out-of-order. AudioPipeline uses a sliding monotonic frontier algorithm:
- Buffers out-of-order frames in an indexed memory map without blocking.
- Advances the contiguous playback frontier dynamically.
- Delivers instant concatenation via pre-allocated contiguous byte buffers.

### B. Two-Tiered Message Broker Architecture
- **In-Memory Zero-Allocation Engine**: Ultra-fast buffered channels for single-instance or embedded operations.
- **Distributed Redis Streams Engine**: Employs consumer groups (`XREADGROUP` / `XACK`) for horizontal worker auto-scaling across Kubernetes pods.

### C. Resilient Asynchronous Workflow State Machine
Long-running jobs transition across explicit lifecycle states:
$$\text{PENDING} \longrightarrow \text{PROCESSING} \longrightarrow \text{COMPLETED} \quad (\text{or } \text{RETRYING} \longrightarrow \text{DEAD\_LETTER})$$
- Workers acquire a 30-second distributed execution lease.
- If a worker crashes mid-execution (e.g. LLM timeout), lease expiration enables standby workers to safely resume the pipeline from the exact last successful step.

---

## 3. Performance & Microbenchmarks

Tested under high concurrency on an AMD Ryzen 8-core CPU:

```bash
$ go test -bench="BenchmarkAudioIngestAndAssembly" -benchmem ./tests/...
BenchmarkAudioIngestAndAssembly-8    1636477    788.9 ns/op    152 B/op    2 allocs/op
PASS
ok      github.com/yoohooshantanu/audio-pipeline/tests    4.467s
```

- **Ingestion Latency**: ~**788 nanoseconds per audio chunk** (sub-microsecond).
- **Allocations**: Only **2 allocs/op** on the contiguous buffer write path.

---

## 4. Quickstart

### Running Locally
```bash
# Clone the repository
git clone https://github.com/yoohooshantanu/audio-pipeline.git
cd audio-pipeline

# Run verified tests
go test -v ./tests/...

# Run standalone service
go run ./cmd/server
```

### Running with Docker Compose
```bash
docker compose up --build -d
```

### Ingesting an Audio Chunk
```bash
curl -X POST http://localhost:8081/v1/streams/audio \
  -H "X-Call-ID: call_abc_123" \
  -H "X-Sequence: 1" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "@sample_frame.raw"
```

### Submitting an Async Post-Call Pipeline Job
```bash
curl -X POST http://localhost:8081/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "call_id": "call_abc_123",
    "tenant_id": "cust_enterprise_01",
    "workflow_type": "post_call_ai_pipeline",
    "payload": {
      "audio_s3_uri": "s3://telephony-recordings/call_abc_123.wav"
    }
  }'
```

---

## 5. Repository Layout

```
audio-pipeline/
├── cmd/server/main.go            # Gateway bootstrap, route registration, graceful shutdown
├── internal/
│   ├── model/types.go            # Domain models: AudioChunk, StreamAck, PipelineJob, States
│   ├── stream/manager.go         # Monotonic audio buffer ordering and memory manager
│   ├── broker/broker.go          # Distributed Redis Streams and In-Memory message broker
│   ├── orchestrator/             # Multi-stage DAG async job worker pool and state store
│   ├── metrics/metrics.go        # Zero-dependency Prometheus exposition telemetry (/metrics)
│   └── logger/logger.go          # Structured slog JSON logger
├── tests/pipeline_test.go        # Concurrency reassembly tests, async workflow assertions, benchmarks
├── docker-compose.yml            # Multi-service deployment with Redis 7
├── Dockerfile                    # Multi-stage Alpine containerization
└── .github/workflows/ci.yml      # GitHub Actions CI with -race detector
```

---

## 6. License

MIT License. Authored by Shantanu.
