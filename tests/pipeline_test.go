package tests

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yoohooshantanu/audio-pipeline/internal/broker"
	"github.com/yoohooshantanu/audio-pipeline/internal/model"
	"github.com/yoohooshantanu/audio-pipeline/internal/orchestrator"
	"github.com/yoohooshantanu/audio-pipeline/internal/stream"
)

func TestAudioBufferOrderingAndAssembly(t *testing.T) {
	buf := stream.NewAudioBuffer("call_test_01")

	// Send out of order: seq 2 then seq 1
	chunk2 := &model.AudioChunk{
		CallID:    "call_test_01",
		Sequence:  2,
		Data:      []byte("WORLD"),
		Timestamp: 1002,
	}
	ack2, err := buf.IngestChunk(chunk2)
	if err != nil {
		t.Fatalf("unexpected error ingesting chunk 2: %v", err)
	}
	if ack2.LastAckSeq != 0 {
		t.Fatalf("expected lastAckSeq 0 (frontier not reached), got %d", ack2.LastAckSeq)
	}

	chunk1 := &model.AudioChunk{
		CallID:    "call_test_01",
		Sequence:  1,
		Data:      []byte("HELLO_"),
		Timestamp: 1001,
	}
	ack1, err := buf.IngestChunk(chunk1)
	if err != nil {
		t.Fatalf("unexpected error ingesting chunk 1: %v", err)
	}
	// After sequence 1 arrives, frontier should jump to 2
	if ack1.LastAckSeq != 2 {
		t.Fatalf("expected contiguous sequence frontier 2, got %d", ack1.LastAckSeq)
	}

	assembled, lastSeq, totalChunks := buf.AssembleContiguousAudio()
	if totalChunks != 2 || lastSeq != 2 {
		t.Fatalf("expected 2 chunks up to seq 2, got count %d, seq %d", totalChunks, lastSeq)
	}
	if string(assembled) != "HELLO_WORLD" {
		t.Fatalf("expected assembled audio 'HELLO_WORLD', got '%s'", string(assembled))
	}
}

func TestWorkflowOrchestratorExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewMemoryBroker(256)
	store := orchestrator.NewMemoryStateStore()
	orch := orchestrator.NewWorkflowOrchestrator(ctx, b, store, "test_jobs", 4)
	orch.Start()
	defer orch.Stop()

	job, err := orch.SubmitJob(ctx, "call_async_01", "tenant_alpha", "post_call_ai_pipeline", map[string]interface{}{
		"duration_sec": 120,
	})
	if err != nil {
		t.Fatalf("failed to submit pipeline job: %v", err)
	}

	// Poll until completed
	deadline := time.Now().Add(3 * time.Second)
	var finalJob *model.PipelineJob
	for time.Now().Before(deadline) {
		j, err := store.GetJob(ctx, job.ID)
		if err == nil && j.State == model.StateCompleted {
			finalJob = j
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if finalJob == nil {
		t.Fatalf("job %s did not reach COMPLETED state within deadline", job.ID)
	}

	if finalJob.CurrentStep != finalJob.TotalSteps {
		t.Fatalf("expected all %d steps executed, got %d", finalJob.TotalSteps, finalJob.CurrentStep)
	}

	if finalJob.Result["sentiment"] != "positive" || finalJob.Result["crm_synced"] != true {
		t.Fatalf("missing expected step outputs in final job result: %+v", finalJob.Result)
	}
}

func TestConcurrentStreamIngestion(t *testing.T) {
	mgr := stream.NewStreamManager(1 * time.Minute)
	callID := "call_concurrent_load"
	buf := mgr.GetOrCreate(callID)

	var wg sync.WaitGroup
	numChunks := 100

	for i := 1; i <= numChunks; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			chunk := &model.AudioChunk{
				CallID:   callID,
				Sequence: seq,
				Data:     bytes.Repeat([]byte{byte(seq)}, 64),
			}
			_, _ = buf.IngestChunk(chunk)
		}(uint64(i))
	}

	wg.Wait()

	assembled, lastSeq, total := buf.AssembleContiguousAudio()
	if lastSeq != uint64(numChunks) || total != numChunks {
		t.Fatalf("expected %d contiguous chunks, got count=%d lastSeq=%d", numChunks, total, lastSeq)
	}
	if len(assembled) != numChunks*64 {
		t.Fatalf("expected %d bytes assembled, got %d", numChunks*64, len(assembled))
	}
}

func BenchmarkAudioIngestAndAssembly(b *testing.B) {
	mgr := stream.NewStreamManager(5 * time.Minute)
	callID := "call_bench_01"
	buf := mgr.GetOrCreate(callID)
	payload := bytes.Repeat([]byte{0xAA}, 160) // 160 bytes = 10ms frame of 8kHz 16-bit PCM

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var seq uint64 = 1
		for pb.Next() {
			chunk := &model.AudioChunk{
				CallID:   callID,
				Sequence: seq,
				Data:     payload,
			}
			_, _ = buf.IngestChunk(chunk)
			seq++
		}
	})
}
