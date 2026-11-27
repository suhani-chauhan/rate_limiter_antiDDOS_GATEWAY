package eventproducer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeWriter records messages instead of talking to Kafka.
type fakeWriter struct {
	mu      sync.Mutex
	msgs    []kafka.Message
	batches int
	err     error
	block   chan struct{} // if non-nil, WriteMessages waits on it
	closed  bool
}

func (f *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.batches++
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func (f *fakeWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

func sample(ip string) Event {
	return NewEvent(ip, "GET", "/home", 200, DecisionAllowed, "test-agent", 5*time.Millisecond)
}

func closeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func TestEmitAndFlushOnClose(t *testing.T) {
	fw := &fakeWriter{}
	p := newWithWriter(Config{BatchSize: 100, FlushInterval: time.Hour}, fw)
	for i := 0; i < 10; i++ {
		if !p.Emit(sample("1.1.1.1")) {
			t.Fatal("emit should succeed")
		}
	}
	ctx, cancel := closeCtx()
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if fw.count() != 10 {
		t.Fatalf("want 10 messages written, got %d", fw.count())
	}
	if s := p.Stats(); s.Enqueued != 10 || s.Sent != 10 || s.Dropped != 0 {
		t.Fatalf("bad stats: %+v", s)
	}
	if !fw.closed {
		t.Fatal("writer should be closed")
	}
}

func TestBatchSizeTriggersFlush(t *testing.T) {
	fw := &fakeWriter{}
	p := newWithWriter(Config{BatchSize: 5, FlushInterval: time.Hour}, fw)
	for i := 0; i < 10; i++ {
		p.Emit(sample("2.2.2.2"))
	}
	deadline := time.Now().Add(2 * time.Second)
	for fw.count() < 10 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fw.count() != 10 {
		t.Fatalf("want 10 flushed by size, got %d", fw.count())
	}
	fw.mu.Lock()
	b := fw.batches
	fw.mu.Unlock()
	if b != 2 {
		t.Fatalf("want 2 batches of 5, got %d", b)
	}
	ctx, cancel := closeCtx()
	defer cancel()
	_ = p.Close(ctx)
}

func TestIntervalTriggersFlush(t *testing.T) {
	fw := &fakeWriter{}
	p := newWithWriter(Config{BatchSize: 1000, FlushInterval: 30 * time.Millisecond}, fw)
	p.Emit(sample("3.3.3.3"))
	deadline := time.Now().Add(2 * time.Second)
	for fw.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fw.count() != 1 {
		t.Fatal("event should flush on timer even though batch is not full")
	}
	ctx, cancel := closeCtx()
	defer cancel()
	_ = p.Close(ctx)
}

func TestDropsWhenBufferFullAndNeverBlocks(t *testing.T) {
	fw := &fakeWriter{block: make(chan struct{})} // writer stuck
	p := newWithWriter(Config{BufferSize: 5, BatchSize: 1, FlushInterval: time.Hour}, fw)

	start := time.Now()
	for i := 0; i < 1000; i++ {
		p.Emit(sample("4.4.4.4"))
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Emit blocked: 1000 calls took %v", elapsed)
	}
	s := p.Stats()
	if s.Dropped == 0 {
		t.Fatal("expected drops when buffer is full")
	}
	if s.Enqueued+s.Dropped != 1000 {
		t.Fatalf("enqueued+dropped must be 1000, got %d", s.Enqueued+s.Dropped)
	}
	close(fw.block) // unstick the writer so Close can finish
	ctx, cancel := closeCtx()
	defer cancel()
	_ = p.Close(ctx)
}

func TestEmitAfterCloseIsDroppedNoPanic(t *testing.T) {
	fw := &fakeWriter{}
	p := newWithWriter(Config{}, fw)
	ctx, cancel := closeCtx()
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if p.Emit(sample("5.5.5.5")) {
		t.Fatal("emit after close must return false")
	}
	if p.Stats().Dropped != 1 {
		t.Fatal("emit after close must count as dropped")
	}
	if err := p.Close(ctx); err != nil { // second close is safe
		t.Fatal(err)
	}
}

func TestWriteFailureCountedNotFatal(t *testing.T) {
	fw := &fakeWriter{err: errors.New("kafka down")}
	p := newWithWriter(Config{BatchSize: 100, FlushInterval: time.Hour}, fw)
	for i := 0; i < 7; i++ {
		p.Emit(sample("6.6.6.6"))
	}
	ctx, cancel := closeCtx()
	defer cancel()
	_ = p.Close(ctx)
	if s := p.Stats(); s.Failed != 7 || s.Sent != 0 {
		t.Fatalf("want failed=7 sent=0, got %+v", s)
	}
}

func TestMessageKeyAndJSONContract(t *testing.T) {
	fw := &fakeWriter{}
	p := newWithWriter(Config{FlushInterval: time.Hour}, fw)
	p.Emit(NewEvent("9.9.9.9", "POST", "/checkout", 429, DecisionRateLimited, "curl/8", 12*time.Millisecond))
	ctx, cancel := closeCtx()
	defer cancel()
	_ = p.Close(ctx)

	if fw.count() != 1 {
		t.Fatalf("want 1 message, got %d", fw.count())
	}
	m := fw.msgs[0]
	if string(m.Key) != "9.9.9.9" {
		t.Fatalf("key must be the IP, got %q", m.Key)
	}
	var got map[string]any
	if err := json.Unmarshal(m.Value, &got); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"timestamp", "ip", "method", "url", "status_code", "decision", "user_agent", "latency_ms"} {
		if _, ok := got[f]; !ok {
			t.Fatalf("missing contract field %q", f)
		}
	}
	if got["decision"] != "rate_limited" || got["latency_ms"].(float64) != 12 {
		t.Fatalf("wrong values: %v", got)
	}
}

func TestConcurrentEmitIsSafe(t *testing.T) {
	fw := &fakeWriter{}
	p := newWithWriter(Config{BufferSize: 100000, BatchSize: 200, FlushInterval: 20 * time.Millisecond}, fw)
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p.Emit(sample("7.7.7.7"))
			}
		}()
	}
	wg.Wait()
	ctx, cancel := closeCtx()
	defer cancel()
	_ = p.Close(ctx)
	s := p.Stats()
	if s.Enqueued != 10000 || s.Sent != 10000 {
		t.Fatalf("want 10000 enqueued and sent, got %+v", s)
	}
}