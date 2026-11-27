package eventproducer

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// Config controls the producer. Zero values get sensible defaults.
type Config struct {
	Brokers       []string      // e.g. []string{"localhost:29092"}
	Topic         string        // default "gateway.traffic.events"
	BufferSize    int           // channel capacity, default 10000
	BatchSize     int           // flush when this many events queued, default 500
	FlushInterval time.Duration // flush at least this often, default 200ms
	WriteTimeout  time.Duration // per-flush Kafka timeout, default 5s
}

// Stats is a snapshot of producer counters.
type Stats struct {
	Enqueued uint64 // accepted into the buffer
	Dropped  uint64 // rejected because buffer full or producer closed
	Sent     uint64 // successfully written to Kafka
	Failed   uint64 // lost because a Kafka write failed
}

// messageWriter is the small part of kafka.Writer we use (lets tests fake it).
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer is an async, batched, non-blocking Kafka event producer.
type Producer struct {
	cfg    Config
	w      messageWriter
	ch     chan Event
	done   chan struct{}
	mu     sync.RWMutex // guards closed + channel send vs close
	closed bool

	enqueued, dropped, sent, failed atomic.Uint64
}

// New creates a producer connected to real Kafka and starts its worker.
func New(cfg Config) (*Producer, error) {
	cfg = withDefaults(cfg)
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("eventproducer: at least one broker is required")
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{}, // same key (IP) -> same partition
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: cfg.WriteTimeout,
		Async:        false, // our worker is the async layer; sync write returns errors
	}
	return newWithWriter(cfg, w), nil
}

func newWithWriter(cfg Config, w messageWriter) *Producer {
	cfg = withDefaults(cfg)
	p := &Producer{
		cfg:  cfg,
		w:    w,
		ch:   make(chan Event, cfg.BufferSize),
		done: make(chan struct{}),
	}
	go p.run()
	return p
}

func withDefaults(c Config) Config {
	if c.Topic == "" {
		c.Topic = "gateway.traffic.events"
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 10000
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 200 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	return c
}

// Emit queues an event. It NEVER blocks: if the buffer is full or the
// producer is closed, the event is dropped and counted. Returns true if queued.
func (p *Producer) Emit(e Event) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		p.dropped.Add(1)
		return false
	}
	select {
	case p.ch <- e:
		p.enqueued.Add(1)
		return true
	default:
		p.dropped.Add(1)
		return false
	}
}

// Stats returns a snapshot of the counters.
func (p *Producer) Stats() Stats {
	return Stats{
		Enqueued: p.enqueued.Load(),
		Dropped:  p.dropped.Load(),
		Sent:     p.sent.Load(),
		Failed:   p.failed.Load(),
	}
}

// Close stops accepting events, drains the buffer, flushes, and closes the
// writer. It waits until done or ctx expires. Safe to call more than once.
func (p *Producer) Close(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.ch) // worker drains remaining events, then exits
	}
	p.mu.Unlock()

	select {
	case <-p.done:
		return p.w.Close()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the background worker: collect -> batch -> flush.
func (p *Producer) run() {
	defer close(p.done)
	batch := make([]kafka.Message, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case e, ok := <-p.ch:
			if !ok { // channel closed and drained
				p.flush(batch)
				return
			}
			value, err := json.Marshal(e)
			if err != nil {
				p.failed.Add(1)
				continue
			}
			batch = append(batch, kafka.Message{Key: []byte(e.IP), Value: value})
			if len(batch) >= p.cfg.BatchSize {
				p.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (p *Producer) flush(batch []kafka.Message) {
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
	defer cancel()
	if err := p.w.WriteMessages(ctx, batch...); err != nil {
		p.failed.Add(uint64(len(batch)))
		log.Printf("eventproducer: write of %d events failed: %v", len(batch), err)
		return
	}
	p.sent.Add(uint64(len(batch)))
}