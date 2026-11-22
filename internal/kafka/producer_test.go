package kafka

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"shorturl/model"

	kafkago "github.com/segmentio/kafka-go"
)

type fallbackModel struct {
	model.ClickModel
	records []*model.Click
	err     error
	calls   int
}

func (m *fallbackModel) BatchInsert(ctx context.Context, records []*model.Click) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("missing fallback timeout")
	}
	m.records = append(m.records, records...)
	m.calls++
	return m.err
}

func TestProducerPartialFallback(t *testing.T) {
	m := &fallbackModel{}
	w := &testWriter{err: kafkago.WriteErrors{nil, errors.New("broker unavailable")}}
	p := newProducer(w, m)
	defer p.Close()
	event := NewClickEvent("abc")
	p.publish([]ClickEvent{NewClickEvent("abc"), event})
	if m.calls != 1 || len(m.records) != 1 {
		t.Fatalf("calls=%d records=%d; want one write for the failed event", m.calls, len(m.records))
	}
	if got := m.records[0]; got.EventId != event.EventID || got.Surl != event.ShortURL || got.Ts != event.Ts {
		t.Fatalf("fallback changed event: %+v", got)
	}
}

func TestProducerSendFallback(t *testing.T) {
	for _, dbErr := range []error{nil, errors.New("database unavailable")} {
		m := &fallbackModel{err: dbErr}
		p := newProducer(&testWriter{err: errors.New("broker unavailable")}, m)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		event := NewClickEvent("abc")
		if err := p.Send(ctx, event); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if m.calls != 1 || len(m.records) != 1 || m.records[0].EventId != event.EventID {
			t.Fatal("fallback missing original event")
		}
	}
}

type pausedClickWriter struct {
	testWriter
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *pausedClickWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	w.once.Do(func() { close(w.started) })
	select {
	case <-w.release:
		return w.testWriter.WriteMessages(ctx, msgs...)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestProducerSendDoesNotWaitForKafka(t *testing.T) {
	m := &fallbackModel{}
	w := &pausedClickWriter{
		testWriter: testWriter{err: errors.New("broker unavailable")},
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	p := newProducer(w, m)
	done := make(chan struct{})
	var sendErr error
	go func() {
		sendErr = p.Send(context.Background(), NewClickEvent("abc"))
		close(done)
	}()
	defer func() {
		close(w.release)
		<-done
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("Kafka write did not start")
	}
	select {
	case <-done:
		if sendErr != nil {
			t.Fatal(sendErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Send waited for Kafka")
	}
}

func TestProducerQueueFullAndClose(t *testing.T) {
	w := &pausedClickWriter{started: make(chan struct{}), release: make(chan struct{})}
	m := &fallbackModel{}
	p := newProducer(w, m)
	var release sync.Once
	unpause := func() { release.Do(func() { close(w.release) }) }
	t.Cleanup(func() {
		unpause()
		_ = p.Close()
	})
	ctx := context.Background()
	if err := p.Send(ctx, NewClickEvent("abc")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("Kafka write did not start")
	}
	for range producerQueueSize {
		if err := p.Send(ctx, NewClickEvent("abc")); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Send(ctx, NewClickEvent("abc")); !errors.Is(err, errClickQueueFull) {
		t.Fatalf("Send with full queue = %v", err)
	}
	unpause()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if len(w.messages) != producerQueueSize+1 || m.calls != 0 {
		t.Fatalf("sent=%d, fallback calls=%d", len(w.messages), m.calls)
	}
	if err := p.Send(ctx, NewClickEvent("abc")); !errors.Is(err, errProducerClosed) {
		t.Fatalf("Send after close = %v", err)
	}
}

func TestProducerFallbackRetry(t *testing.T) {
	m := &testClickModel{insertErrors: []error{driver.ErrBadConn, nil}}
	p := &Producer{ctx: context.Background(), clickModel: m}
	start := time.Now()
	if err := p.save([]ClickEvent{NewClickEvent("abc")}); err != nil {
		t.Fatal(err)
	}
	if m.inserts != 2 || time.Since(start) < time.Second {
		t.Fatalf("inserts=%d", m.inserts)
	}
}
