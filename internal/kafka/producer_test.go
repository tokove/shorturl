package kafka

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	kafkago "github.com/segmentio/kafka-go"
	"shorturl/model"
	"testing"
	"time"
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

func TestProducerCompletion(t *testing.T) {
	for _, failed := range []bool{false, true} {
		m := &fallbackModel{}
		p, err := NewProducer([]string{"localhost:9092"}, "clicks", m)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		var sendErr error
		if failed {
			sendErr = errors.New("broker unavailable")
		}
		messages := []kafkago.Message{batchMessage(0), batchMessage(1)}
		p.writer.(*kafkago.Writer).Completion(messages, sendErr)
		if !failed {
			if m.calls != 0 {
				t.Fatal("successful send wrote database")
			}
			continue
		}
		if m.calls != 1 || len(m.records) != 2 {
			t.Fatalf("calls=%d records=%d", m.calls, len(m.records))
		}
		for i, msg := range messages {
			event := ClickEvent{}
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				t.Fatal(err)
			}
			if m.records[i].EventId != event.EventID || m.records[i].Surl != event.ShortURL || m.records[i].Ts != event.Ts {
				t.Fatal("event changed during fallback")
			}
		}
	}
}

func TestProducerSendFallback(t *testing.T) {
	for _, dbErr := range []error{nil, errors.New("database unavailable")} {
		m := &fallbackModel{err: dbErr}
		p := &Producer{writer: &testWriter{err: errors.New("broker unavailable")}, clickModel: m}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		event := NewClickEvent("abc")
		err := p.Send(ctx, event)
		if !errors.Is(err, dbErr) {
			t.Fatalf("Send=%v want=%v", err, dbErr)
		}
		if m.calls != 1 || m.records[0].EventId != event.EventID {
			t.Fatal("fallback missing original event")
		}
	}
}

func TestProducerFallbackRetry(t *testing.T) {
	m := &testClickModel{insertErrors: []error{driver.ErrBadConn, nil}}
	p := &Producer{clickModel: m}
	start := time.Now()
	if err := p.save([]kafkago.Message{batchMessage(0)}); err != nil {
		t.Fatal(err)
	}
	if m.inserts != 2 || time.Since(start) < time.Second {
		t.Fatalf("inserts=%d", m.inserts)
	}
}
