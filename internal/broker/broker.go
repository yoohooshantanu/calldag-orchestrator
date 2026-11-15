package broker

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/voicepipe/internal/model"
)

// MessageBroker defines the interface for distributed streaming and message queuing
type MessageBroker interface {
	PublishStream(ctx context.Context, topic string, payload interface{}) error
	SubscribeStream(ctx context.Context, topic string, group string, consumer string, handler func(data []byte) error) error
	EnqueueJob(ctx context.Context, queueName string, job *model.PipelineJob) error
	DequeueJob(ctx context.Context, queueName string, timeout time.Duration) (*model.PipelineJob, error)
	Close() error
}

// MemoryBroker is an in-memory high-throughput broker using buffered channels & pub-sub maps
type MemoryBroker struct {
	mu         sync.RWMutex
	queues     map[string]chan *model.PipelineJob
	topics     map[string][]chan []byte
	bufferSize int
	closed     bool
}

func NewMemoryBroker(bufferSize int) *MemoryBroker {
	if bufferSize <= 0 {
		bufferSize = 2048
	}
	return &MemoryBroker{
		queues:     make(map[string]chan *model.PipelineJob),
		topics:     make(map[string][]chan []byte),
		bufferSize: bufferSize,
	}
}

func (m *MemoryBroker) getOrCreateQueue(name string) chan *model.PipelineJob {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch, ok := m.queues[name]
	if !ok {
		ch = make(chan *model.PipelineJob, m.bufferSize)
		m.queues[name] = ch
	}
	return ch
}

func (m *MemoryBroker) PublishStream(ctx context.Context, topic string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	m.mu.RLock()
	subs, ok := m.topics[topic]
	m.mu.RUnlock()

	if !ok {
		return nil
	}

	for _, ch := range subs {
		select {
		case ch <- data:
		default:
			// Non-blocking drop for slow real-time consumer to preserve latency
		}
	}
	return nil
}

func (m *MemoryBroker) SubscribeStream(ctx context.Context, topic string, group string, consumer string, handler func(data []byte) error) error {
	m.mu.Lock()
	ch := make(chan []byte, 512)
	m.topics[topic] = append(m.topics[topic], ch)
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			subs := m.topics[topic]
			for i, sub := range subs {
				if sub == ch {
					m.topics[topic] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
			m.mu.Unlock()
			close(ch)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-ch:
				if !ok {
					return
				}
				_ = handler(data)
			}
		}
	}()
	return nil
}

func (m *MemoryBroker) EnqueueJob(ctx context.Context, queueName string, job *model.PipelineJob) error {
	q := m.getOrCreateQueue(queueName)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case q <- job:
		return nil
	default:
		return model.ErrQueueSaturated
	}
}

func (m *MemoryBroker) DequeueJob(ctx context.Context, queueName string, timeout time.Duration) (*model.PipelineJob, error) {
	q := m.getOrCreateQueue(queueName)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case job := <-q:
		return job, nil
	case <-time.After(timeout):
		return nil, nil // non-blocking poll timeout
	}
}

func (m *MemoryBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// RedisStreamBroker implements distributed Redis Streams (XADD/XREADGROUP) & LPUSH/BRPOP
type RedisStreamBroker struct {
	client *redis.Client
}

func NewRedisStreamBroker(client *redis.Client) *RedisStreamBroker {
	return &RedisStreamBroker{client: client}
}

func (r *RedisStreamBroker) PublishStream(ctx context.Context, topic string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:" + topic,
		Values: map[string]interface{}{"payload": data},
		MaxLen: 5000,
		Approx: true,
	}).Err()
}

func (r *RedisStreamBroker) SubscribeStream(ctx context.Context, topic string, group string, consumer string, handler func(data []byte) error) error {
	streamKey := "stream:" + topic
	_ = r.client.XGroupCreateMkStream(ctx, streamKey, group, "0").Err()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				entries, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: consumer,
					Streams:  []string{streamKey, ">"},
					Count:    10,
					Block:    2 * time.Second,
				}).Result()

				if err != nil {
					select {
					case <-time.After(500 * time.Millisecond):
					case <-ctx.Done():
						return
					}
					continue
				}

				for _, stream := range entries {
					for _, msg := range stream.Messages {
						if raw, ok := msg.Values["payload"].(string); ok {
							if err := handler([]byte(raw)); err == nil {
								r.client.XAck(ctx, streamKey, group, msg.ID)
							}
						}
					}
				}
			}
		}
	}()
	return nil
}

func (r *RedisStreamBroker) EnqueueJob(ctx context.Context, queueName string, job *model.PipelineJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return r.client.LPush(ctx, "jobqueue:"+queueName, data).Err()
}

func (r *RedisStreamBroker) DequeueJob(ctx context.Context, queueName string, timeout time.Duration) (*model.PipelineJob, error) {
	res, err := r.client.BRPop(ctx, timeout, "jobqueue:"+queueName).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	if len(res) < 2 {
		return nil, nil
	}

	var job model.PipelineJob
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *RedisStreamBroker) Close() error {
	return r.client.Close()
}
