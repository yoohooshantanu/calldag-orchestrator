package model

import (
	"errors"
	"time"
)

var (
	ErrQueueSaturated   = errors.New("pipeline queue saturated: backpressure shedding")
	ErrJobNotFound      = errors.New("job not found")
	ErrInvalidStep      = errors.New("invalid execution step for current state")
	ErrLeaseExpired     = errors.New("worker lease expired")
	ErrDuplicateAudioSeq= errors.New("duplicate or out-of-order audio sequence")
)

type JobState string

const (
	StatePending    JobState = "PENDING"
	StateStreaming  JobState = "STREAMING"
	StateProcessing JobState = "PROCESSING"
	StateCompleted  JobState = "COMPLETED"
	StateFailed     JobState = "FAILED"
	StateDeadLetter JobState = "DEAD_LETTER"
)

// PipelineJob models an asynchronous multi-stage post-call workflow
type PipelineJob struct {
	ID           string                 `json:"id"`
	CallID       string                 `json:"call_id"`
	TenantID     string                 `json:"tenant_id"`
	WorkflowType string                 `json:"workflow_type"`
	State        JobState               `json:"state"`
	CurrentStep  int                    `json:"current_step"`
	TotalSteps   int                    `json:"total_steps"`
	Payload      map[string]interface{} `json:"payload"`
	Result       map[string]interface{} `json:"result,omitempty"`
	RetryCount   int                    `json:"retry_count"`
	MaxRetries   int                    `json:"max_retries"`
	LastError    string                 `json:"last_error,omitempty"`
	WorkerID     string                 `json:"worker_id,omitempty"`
	LeaseExpires *time.Time             `json:"lease_expires,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
}

// AudioChunk represents a binary frame of PCM or Opus audio streaming from carriers
type AudioChunk struct {
	CallID    string `json:"call_id"`
	Sequence  uint64 `json:"sequence"`
	Timestamp int64  `json:"timestamp"`
	Data      []byte `json:"data"`
	Codec     string `json:"codec"` // e.g. "pcm_s16le", "opus"
	IsFinal   bool   `json:"is_final"`
}

// StreamAck is sent back to telephony stream client for flow-control & backpressure
type StreamAck struct {
	CallID        string `json:"call_id"`
	LastAckSeq    uint64 `json:"last_ack_seq"`
	BufferBacklog int    `json:"buffer_backlog"`
	Accepted      bool   `json:"accepted"`
	Message       string `json:"message,omitempty"`
}
