package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Metrics struct {
	audioChunksReceived uint64
	audioBytesTotal     uint64
	jobsEnqueuedTotal   uint64
	jobsCompletedTotal  uint64
	jobsRetriedTotal    uint64
	jobsDLQTotal        uint64
	activeWorkers       int64
	streamBacklog       int64
	jobDurationSumMs    uint64
	jobDurationCount    uint64
}

var DefaultMetrics = &Metrics{}

func (m *Metrics) IncAudioChunk(size int) {
	atomic.AddUint64(&m.audioChunksReceived, 1)
	atomic.AddUint64(&m.audioBytesTotal, uint64(size))
}

func (m *Metrics) IncJobEnqueued() {
	atomic.AddUint64(&m.jobsEnqueuedTotal, 1)
}

func (m *Metrics) IncJobCompleted(dur time.Duration) {
	atomic.AddUint64(&m.jobsCompletedTotal, 1)
	atomic.AddUint64(&m.jobDurationSumMs, uint64(dur.Milliseconds()))
	atomic.AddUint64(&m.jobDurationCount, 1)
}

func (m *Metrics) IncJobRetried() {
	atomic.AddUint64(&m.jobsRetriedTotal, 1)
}

func (m *Metrics) IncJobDLQ() {
	atomic.AddUint64(&m.jobsDLQTotal, 1)
}

func (m *Metrics) SetActiveWorkers(val int64) {
	atomic.StoreInt64(&m.activeWorkers, val)
}

func (m *Metrics) SetStreamBacklog(val int64) {
	atomic.StoreInt64(&m.streamBacklog, val)
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		fmt.Fprintf(w, "# HELP voicepipe_audio_chunks_total Total incoming audio chunks processed\n")
		fmt.Fprintf(w, "# TYPE voicepipe_audio_chunks_total counter\n")
		fmt.Fprintf(w, "voicepipe_audio_chunks_total %d\n\n", atomic.LoadUint64(&m.audioChunksReceived))

		fmt.Fprintf(w, "# HELP voicepipe_audio_bytes_total Total raw audio bytes ingested\n")
		fmt.Fprintf(w, "# TYPE voicepipe_audio_bytes_total counter\n")
		fmt.Fprintf(w, "voicepipe_audio_bytes_total %d\n\n", atomic.LoadUint64(&m.audioBytesTotal))

		fmt.Fprintf(w, "# HELP voicepipe_jobs_total Pipeline async jobs lifecycle counters\n")
		fmt.Fprintf(w, "# TYPE voicepipe_jobs_total counter\n")
		fmt.Fprintf(w, "voicepipe_jobs_total{status=\"enqueued\"} %d\n", atomic.LoadUint64(&m.jobsEnqueuedTotal))
		fmt.Fprintf(w, "voicepipe_jobs_total{status=\"completed\"} %d\n", atomic.LoadUint64(&m.jobsCompletedTotal))
		fmt.Fprintf(w, "voicepipe_jobs_total{status=\"retried\"} %d\n", atomic.LoadUint64(&m.jobsRetriedTotal))
		fmt.Fprintf(w, "voicepipe_jobs_total{status=\"dlq\"} %d\n\n", atomic.LoadUint64(&m.jobsDLQTotal))

		fmt.Fprintf(w, "# HELP voicepipe_active_workers Currently executing background workers\n")
		fmt.Fprintf(w, "# TYPE voicepipe_active_workers gauge\n")
		fmt.Fprintf(w, "voicepipe_active_workers %d\n\n", atomic.LoadInt64(&m.activeWorkers))

		fmt.Fprintf(w, "# HELP voicepipe_stream_backlog Audio buffer backlog waiting for consumption\n")
		fmt.Fprintf(w, "# TYPE voicepipe_stream_backlog gauge\n")
		fmt.Fprintf(w, "voicepipe_stream_backlog %d\n\n", atomic.LoadInt64(&m.streamBacklog))

		sum := atomic.LoadUint64(&m.jobDurationSumMs)
		count := atomic.LoadUint64(&m.jobDurationCount)
		fmt.Fprintf(w, "# HELP voicepipe_job_duration_ms Total job execution duration in ms\n")
		fmt.Fprintf(w, "# TYPE voicepipe_job_duration_ms summary\n")
		fmt.Fprintf(w, "voicepipe_job_duration_ms_sum %d\n", sum)
		fmt.Fprintf(w, "voicepipe_job_duration_ms_count %d\n", count)
	}
}
