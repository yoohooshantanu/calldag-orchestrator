package stream

import (
	"sync"
	"time"

	"github.com/yoohooshantanu/voicepipe/internal/logger"
	"github.com/yoohooshantanu/voicepipe/internal/metrics"
	"github.com/yoohooshantanu/voicepipe/internal/model"
)

// Lock hierarchy: StreamManager.mu > AudioBuffer.mu.
// Always acquire sm.mu before b.mu; never acquire sm.mu while holding b.mu.

// AudioBuffer maintains ordered chunks for a live telephony call session
type AudioBuffer struct {
	mu           sync.RWMutex
	callID       string
	expectedSeq  uint64
	chunks       map[uint64]*model.AudioChunk
	totalBytes   int
	isClosed     bool
	lastActivity time.Time
}

func NewAudioBuffer(callID string) *AudioBuffer {
	return &AudioBuffer{
		callID:       callID,
		expectedSeq:  1,
		chunks:       make(map[uint64]*model.AudioChunk),
		lastActivity: time.Now(),
	}
}

const MaxChunksPerCall = 50000

// IngestChunk safely buffers incoming frames, checking sequence monotonicity
func (b *AudioBuffer) IngestChunk(chunk *model.AudioChunk) (*model.StreamAck, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastActivity = time.Now()

	// Enforce maximum buffer capacity per call to prevent memory exhaustion
	if len(b.chunks) >= MaxChunksPerCall {
		return &model.StreamAck{
			CallID:        b.callID,
			LastAckSeq:    b.expectedSeq - 1,
			BufferBacklog: len(b.chunks),
			Accepted:      false,
			Message:       "stream buffer capacity exceeded",
		}, model.ErrQueueSaturated
	}

	// Check for backwards duplicate sequence
	if chunk.Sequence < b.expectedSeq {
		return &model.StreamAck{
			CallID:        b.callID,
			LastAckSeq:    b.expectedSeq - 1,
			BufferBacklog: len(b.chunks),
			Accepted:      false,
			Message:       "duplicate sequence already acknowledged",
		}, model.ErrDuplicateAudioSeq
	}

	// Prevent duplicate future chunk from double-counting bytes
	if existing, exists := b.chunks[chunk.Sequence]; exists {
		b.totalBytes -= len(existing.Data)
	}

	b.chunks[chunk.Sequence] = chunk
	b.totalBytes += len(chunk.Data)

	metrics.DefaultMetrics.IncAudioChunk(len(chunk.Data))

	// Monotonically advance contiguous sequence frontier
	for {
		if _, ok := b.chunks[b.expectedSeq]; ok {
			b.expectedSeq++
		} else {
			break
		}
	}

	ack := &model.StreamAck{
		CallID:        b.callID,
		LastAckSeq:    b.expectedSeq - 1,
		BufferBacklog: len(b.chunks),
		Accepted:      true,
	}

	if chunk.IsFinal {
		b.isClosed = true
	}

	return ack, nil
}

// AssembleContiguousAudio recombines in-order frames into a unified payload for async jobs
func (b *AudioBuffer) AssembleContiguousAudio() ([]byte, uint64, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]byte, 0, b.totalBytes)
	var lastSeq uint64 = 0

	for seq := uint64(1); seq < b.expectedSeq; seq++ {
		if chunk, ok := b.chunks[seq]; ok {
			result = append(result, chunk.Data...)
			lastSeq = seq
		}
	}

	return result, lastSeq, len(b.chunks)
}

// StreamManager manages multi-tenant active audio streaming sessions
type StreamManager struct {
	mu      sync.RWMutex
	buffers map[string]*AudioBuffer
	ttl     time.Duration
	stopCh  chan struct{}
}

func NewStreamManager(ttl time.Duration) *StreamManager {
	sm := &StreamManager{
		buffers: make(map[string]*AudioBuffer),
		ttl:     ttl,
		stopCh:  make(chan struct{}),
	}
	go sm.evictionLoop()
	return sm
}

func (sm *StreamManager) Stop() {
	select {
	case <-sm.stopCh:
		// already closed
	default:
		close(sm.stopCh)
	}
}

func (sm *StreamManager) GetOrCreate(callID string) *AudioBuffer {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	buf, ok := sm.buffers[callID]
	if !ok {
		buf = NewAudioBuffer(callID)
		sm.buffers[callID] = buf
	}
	return buf
}

func (sm *StreamManager) Get(callID string) (*AudioBuffer, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	buf, ok := sm.buffers[callID]
	return buf, ok
}

func (sm *StreamManager) ActiveCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.buffers)
}

func (sm *StreamManager) evictionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.stopCh:
			return
		case <-ticker.C:
			sm.mu.Lock()
			now := time.Now()
			for id, b := range sm.buffers {
				b.mu.RLock()
				expired := now.Sub(b.lastActivity) > sm.ttl
				b.mu.RUnlock()

				if expired {
					delete(sm.buffers, id)
					logger.L.Debug("evicted_stale_audio_stream", "call_id", id)
				}
			}
			metrics.DefaultMetrics.SetStreamBacklog(int64(len(sm.buffers)))
			sm.mu.Unlock()
		}
	}
}
