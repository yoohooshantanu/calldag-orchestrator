package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yoohooshantanu/voicepipe/internal/broker"
	"github.com/yoohooshantanu/voicepipe/internal/logger"
	"github.com/yoohooshantanu/voicepipe/internal/metrics"
	"github.com/yoohooshantanu/voicepipe/internal/model"
)

// StepExecutor defines execution logic for an individual step in a DAG workflow
type StepExecutor func(ctx context.Context, job *model.PipelineJob) error

// StateStore persists job state transitions and audit logs
type StateStore interface {
	SaveJob(ctx context.Context, job *model.PipelineJob) error
	GetJob(ctx context.Context, id string) (*model.PipelineJob, error)
	ListJobsByCall(ctx context.Context, callID string) ([]*model.PipelineJob, error)
}

// MemoryStateStore provides thread-safe in-memory state tracking
type MemoryStateStore struct {
	mu   sync.RWMutex
	jobs map[string]*model.PipelineJob
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{jobs: make(map[string]*model.PipelineJob)}
}

func (s *MemoryStateStore) SaveJob(ctx context.Context, job *model.PipelineJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *MemoryStateStore) GetJob(ctx context.Context, id string) (*model.PipelineJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, model.ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

func (s *MemoryStateStore) ListJobsByCall(ctx context.Context, callID string) ([]*model.PipelineJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var list []*model.PipelineJob
	for _, j := range s.jobs {
		if j.CallID == callID {
			cp := *j
			list = append(list, &cp)
		}
	}
	return list, nil
}

// WorkflowOrchestrator manages multi-stage async job execution pipelines
type WorkflowOrchestrator struct {
	broker     broker.MessageBroker
	store      StateStore
	queueName  string
	workerID   string
	numWorkers int
	steps      map[string][]StepExecutor
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewWorkflowOrchestrator(
	ctx context.Context,
	b broker.MessageBroker,
	store StateStore,
	queueName string,
	numWorkers int,
) *WorkflowOrchestrator {
	pCtx, cancel := context.WithCancel(ctx)
	orch := &WorkflowOrchestrator{
		broker:     b,
		store:      store,
		queueName:  queueName,
		workerID:   "wrk_" + uuid.NewString()[:8],
		numWorkers: numWorkers,
		steps:      make(map[string][]StepExecutor),
		ctx:        pCtx,
		cancel:     cancel,
	}

	orch.registerStandardSteps()
	return orch
}

func (o *WorkflowOrchestrator) registerStandardSteps() {
	// Standard Post-Call AI Processing Pipeline
	o.steps["post_call_ai_pipeline"] = []StepExecutor{
		// Step 1: Audio Concatenation & Validation
		func(ctx context.Context, job *model.PipelineJob) error {
			time.Sleep(5 * time.Millisecond) // Simulated audio frame stitching
			return nil
		},
		// Step 2: Speech Transcription & Diarization
		func(ctx context.Context, job *model.PipelineJob) error {
			time.Sleep(10 * time.Millisecond) // Simulated speech-to-text API call
			if job.Result == nil {
				job.Result = make(map[string]interface{})
			}
			job.Result["transcription"] = "Customer inquired about service cancellation and billing cycle."
			return nil
		},
		// Step 3: LLM Conversation Summary & Sentiment
		func(ctx context.Context, job *model.PipelineJob) error {
			time.Sleep(10 * time.Millisecond) // Simulated LLM analysis
			job.Result["summary"] = "Customer resolved with standard retention offer."
			job.Result["sentiment"] = "positive"
			return nil
		},
		// Step 4: CRM / Webhook Sync
		func(ctx context.Context, job *model.PipelineJob) error {
			time.Sleep(5 * time.Millisecond) // Simulated CRM update
			job.Result["crm_synced"] = true
			return nil
		},
	}
}

// SubmitJob starts a new asynchronous workflow
func (o *WorkflowOrchestrator) SubmitJob(
	ctx context.Context,
	callID string,
	tenantID string,
	wfType string,
	payload map[string]interface{},
) (*model.PipelineJob, error) {
	steps, ok := o.steps[wfType]
	if !ok {
		return nil, fmt.Errorf("unknown workflow type: %s", wfType)
	}

	now := time.Now().UTC()
	job := &model.PipelineJob{
		ID:           "job_" + uuid.NewString()[:12],
		CallID:       callID,
		TenantID:     tenantID,
		WorkflowType: wfType,
		State:        model.StatePending,
		CurrentStep:  0,
		TotalSteps:   len(steps),
		Payload:      payload,
		Result:       make(map[string]interface{}),
		RetryCount:   0,
		MaxRetries:   3,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := o.store.SaveJob(ctx, job); err != nil {
		return nil, err
	}

	if err := o.broker.EnqueueJob(ctx, o.queueName, job); err != nil {
		return nil, err
	}

	metrics.DefaultMetrics.IncJobEnqueued()
	logger.L.Info("pipeline_job_enqueued", "job_id", job.ID, "call_id", callID, "workflow", wfType)

	return job, nil
}

// Start begins processing jobs with the worker pool
func (o *WorkflowOrchestrator) Start() {
	metrics.DefaultMetrics.SetActiveWorkers(int64(o.numWorkers))
	for i := 0; i < o.numWorkers; i++ {
		o.wg.Add(1)
		go o.workerLoop(i)
	}
}

func (o *WorkflowOrchestrator) workerLoop(workerNum int) {
	defer o.wg.Done()

	for {
		select {
		case <-o.ctx.Done():
			return
		default:
			job, err := o.broker.DequeueJob(o.ctx, o.queueName, 2*time.Second)
			if err != nil || job == nil {
				continue
			}

			o.executeJobPipeline(job)
		}
	}
}

func (o *WorkflowOrchestrator) executeJobPipeline(job *model.PipelineJob) {
	start := time.Now()
	steps, ok := o.steps[job.WorkflowType]
	if !ok {
		return
	}

	job.State = model.StateProcessing
	job.WorkerID = o.workerID
	leaseExp := time.Now().Add(30 * time.Second)
	job.LeaseExpires = &leaseExp
	job.UpdatedAt = time.Now().UTC()
	_ = o.store.SaveJob(o.ctx, job)

	for stepIdx := job.CurrentStep; stepIdx < len(steps); stepIdx++ {
		exec := steps[stepIdx]
		err := exec(o.ctx, job)
		if err != nil {
			o.handleStepFailure(job, err)
			return
		}
		job.CurrentStep = stepIdx + 1
		job.UpdatedAt = time.Now().UTC()
		_ = o.store.SaveJob(o.ctx, job)
	}

	// Workflow Completed Successfully
	now := time.Now().UTC()
	job.State = model.StateCompleted
	job.CompletedAt = &now
	job.UpdatedAt = now
	_ = o.store.SaveJob(o.ctx, job)

	dur := time.Since(start)
	metrics.DefaultMetrics.IncJobCompleted(dur)
	logger.L.Info("pipeline_job_completed", "job_id", job.ID, "call_id", job.CallID, "duration_ms", dur.Milliseconds())
}

func (o *WorkflowOrchestrator) handleStepFailure(job *model.PipelineJob, err error) {
	job.RetryCount++
	job.LastError = err.Error()
	job.UpdatedAt = time.Now().UTC()

	if job.RetryCount <= job.MaxRetries {
		metrics.DefaultMetrics.IncJobRetried()
		job.State = model.StatePending
		_ = o.store.SaveJob(o.ctx, job)

		// Exponential backoff wait before re-enqueue
		backoff := time.Duration(1<<job.RetryCount) * 100 * time.Millisecond
		go func() {
			time.Sleep(backoff)
			_ = o.broker.EnqueueJob(context.Background(), o.queueName, job)
		}()
		logger.L.Warn("pipeline_job_retry", "job_id", job.ID, "attempt", job.RetryCount, "error", err.Error())
	} else {
		// DLQ routing
		metrics.DefaultMetrics.IncJobDLQ()
		job.State = model.StateDeadLetter
		_ = o.store.SaveJob(o.ctx, job)
		logger.L.Error("pipeline_job_dead_letter", "job_id", job.ID, "error", err.Error())
	}
}

func (o *WorkflowOrchestrator) Stop() {
	o.cancel()
	o.wg.Wait()
	metrics.DefaultMetrics.SetActiveWorkers(0)
}
