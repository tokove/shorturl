package kafka

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"shorturl/model"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

type testReader struct {
	message   kafkago.Message
	fetched   bool
	commits   int
	commitErr error
}

func (r *testReader) FetchMessage(context.Context) (kafkago.Message, error) {
	if r.fetched {
		return kafkago.Message{}, context.Canceled
	}
	r.fetched = true
	return r.message, nil
}

func (r *testReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	r.commits += len(messages)
	return r.commitErr
}

func (r *testReader) Close() error { return nil }

type testClickModel struct {
	model.ClickModel
	records      []*model.Click
	record       *model.Click
	insertErr    error
	inserts      int
	insertErrors []error
}

func (m *testClickModel) Insert(_ context.Context, record *model.Click) (sql.Result, error) {
	m.record = record
	m.inserts++
	if len(m.insertErrors) > 0 {
		err := m.insertErrors[0]
		m.insertErrors = m.insertErrors[1:]
		return nil, err
	}
	return nil, m.insertErr
}

func TestConsumerCommit(t *testing.T) {
	writeErr := errors.New("database unavailable")
	commitErr := errors.New("commit failed")
	cases := []struct {
		name      string
		insertErr error
		commitErr error
		commits   int
		wantErr   error
	}{
		{"inserted", nil, nil, 1, context.Canceled},
		{"unexpected duplicate error", &mysql.MySQLError{Number: 1062}, nil, 0, nil},
		{"write failed", writeErr, nil, 0, writeErr},
		{"commit failed", nil, commitErr, 4, commitErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &testReader{
				message: kafkago.Message{Topic: "shorturl_clicks", Partition: 0, Offset: 0,
					Value: []byte(`{"event_id":"b5b03269-fd35-4467-9bee-4fcd163fd242","short_url":"Ab1","ts":1}`)},
				commitErr: tc.commitErr,
			}
			clickModel := &testClickModel{insertErr: tc.insertErr}
			consumer := &Consumer{batchSize: 1, flushInterval: time.Second, reader: reader, clickModel: clickModel}
			err := consumer.Run(context.Background())
			wantErr := tc.wantErr
			if wantErr == nil {
				wantErr = tc.insertErr
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if reader.commits != tc.commits {
				t.Fatalf("commits = %d, want %d", reader.commits, tc.commits)
			}
			if clickModel.inserts != 1 {
				t.Fatalf("unexpected repeated insert: %d", clickModel.inserts)
			}
			record := clickModel.record
			if record == nil || record.EventId != "b5b03269-fd35-4467-9bee-4fcd163fd242" ||
				record.Surl != "Ab1" || record.Ts != 1 {
				t.Fatalf("incorrect message identity: %+v", record)
			}
		})
	}
}

func TestConsumerPreservesEventID(t *testing.T) {
	event := NewClickEvent("Ab1")
	if _, err := uuid.Parse(event.EventID); err != nil {
		t.Fatalf("invalid event ID: %v", err)
	}
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	// 同一事件换到另一个 Topic 或 offset，仍使用生产端的 ID。
	for _, offset := range []int64{1, 100} {
		reader := &testReader{message: kafkago.Message{
			Topic: "replay", Partition: 2, Offset: offset, Value: value,
		}}
		clickModel := &testClickModel{}
		consumer := &Consumer{batchSize: 1, flushInterval: time.Second, reader: reader, clickModel: clickModel}
		if err := consumer.Run(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if clickModel.record == nil || clickModel.record.EventId != event.EventID {
			t.Fatalf("event ID was changed: %+v", clickModel.record)
		}
	}
}

func TestConsumerRetriesWrite(t *testing.T) {
	for _, tc := range []struct {
		name             string
		failures         []error
		finalErr         error
		inserts, commits int
		wantErr          error
	}{
		{"recovered", []error{driver.ErrBadConn, nil}, nil, 2, 1, context.Canceled},

		{"exhausted", nil, driver.ErrBadConn, 4, 0, driver.ErrBadConn},
		{"permanent", nil, &mysql.MySQLError{Number: 1406}, 1, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &testReader{message: kafkago.Message{Value: []byte(`{"event_id":"test-id","short_url":"Ab1","ts":1}`)}}
			clickModel := &testClickModel{insertErrors: tc.failures, insertErr: tc.finalErr}
			consumer := &Consumer{batchSize: 1, flushInterval: time.Second, reader: reader, clickModel: clickModel}
			err := consumer.Run(context.Background())
			wantErr := tc.wantErr
			if wantErr == nil {
				wantErr = tc.finalErr
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("error = %v, want %v", err, wantErr)
			}
			if clickModel.inserts != tc.inserts || reader.commits != tc.commits {
				t.Fatalf("inserts=%d commits=%d", clickModel.inserts, reader.commits)
			}
		})
	}
}

func TestRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := retry(ctx, func() error { calls++; cancel(); return driver.ErrBadConn }, isRetryableDBError)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("error=%v calls=%d", err, calls)
	}
}

type testWriter struct {
	messages []kafkago.Message
	err      error
	calls    int
	failures int
}

func (w *testWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.calls++
	if w.failures > 0 {
		w.failures--
		return errors.New("temporary dead letter failure")
	}
	if w.err != nil {
		return w.err
	}
	w.messages = append(w.messages, messages...)
	return nil
}
func (w *testWriter) Close() error { return nil }

func TestConsumerDeadLetter(t *testing.T) {
	sendErr := errors.New("dead letter unavailable")
	commitErr := errors.New("commit unavailable")
	for _, tc := range []struct {
		name                     string
		sendErr, errorCommit     error
		failures, calls, commits int
		wantErr                  error
	}{
		{"sent", nil, nil, 0, 1, 1, context.Canceled},
		{"recovered", nil, nil, 1, 2, 1, context.Canceled},
		{"send failed", sendErr, nil, 0, 4, 0, sendErr},
		{"commit failed", nil, commitErr, 0, 1, 4, commitErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := kafkago.Message{Topic: "clicks", Partition: 2, Offset: 3, Key: []byte("Ab1"), Value: []byte("{bad")}
			reader := &testReader{message: msg, commitErr: tc.errorCommit}
			writer := &testWriter{err: tc.sendErr, failures: tc.failures}
			clickModel := &testClickModel{}
			consumer := &Consumer{batchSize: 1, flushInterval: time.Second, reader: reader, deadLetter: writer, clickModel: clickModel}
			err := consumer.Run(context.Background())
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error=%v want=%v", err, tc.wantErr)
			}
			if reader.commits != tc.commits || writer.calls != tc.calls || clickModel.inserts != 0 {
				t.Fatalf("commits=%d sends=%d inserts=%d", reader.commits, writer.calls, clickModel.inserts)
			}
			if len(writer.messages) > 0 {
				sent := writer.messages[0]
				if string(sent.Key) != "Ab1" || string(sent.Value) != "{bad" {
					t.Fatal("original message changed")
				}
				headers := map[string]string{}
				for _, h := range sent.Headers {
					headers[h.Key] = string(h.Value)
				}
				if headers["source-topic"] != "clicks" || headers["source-partition"] != "2" ||
					headers["source-offset"] != "3" || headers["error"] == "" {
					t.Fatalf("headers=%v", headers)
				}
			}
		})
	}
}

func (m *testClickModel) BatchInsert(ctx context.Context, records []*model.Click) error {
	m.records = append([]*model.Click(nil), records...)
	_, err := m.Insert(ctx, records[0])
	return err
}

type batchReader struct {
	messages     []kafkago.Message
	committed    []kafkago.Message
	wait         bool
	beforeCommit func()
}

func (r *batchReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if len(r.messages) > 0 {
		msg := r.messages[0]
		r.messages = r.messages[1:]
		return msg, nil
	}
	if r.wait && len(r.committed) == 0 {
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	return kafkago.Message{}, context.Canceled
}
func (r *batchReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	if r.beforeCommit != nil {
		r.beforeCommit()
	}
	r.committed = append(r.committed, messages...)
	return nil
}
func (r *batchReader) Close() error { return nil }
func batchMessage(offset int64) kafkago.Message {
	value, _ := json.Marshal(NewClickEvent("Ab1"))
	return kafkago.Message{Topic: "clicks", Partition: 0, Offset: offset, Value: value}
}
func TestConsumerBatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		size, count int
		wait        bool
	}{
		{"size", 2, 2, false}, {"time", 100, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &batchReader{wait: tc.wait}
			for i := 0; i < tc.count; i++ {
				reader.messages = append(reader.messages, batchMessage(int64(i)))
			}
			m := &testClickModel{}
			reader.beforeCommit = func() {
				if len(m.records) != tc.count {
					t.Fatal("commit before whole batch inserted")
				}
			}
			c := &Consumer{reader: reader, clickModel: m, batchSize: tc.size, flushInterval: 20 * time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := c.Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			if m.inserts != 1 || len(m.records) != tc.count || len(reader.committed) != tc.count {
				t.Fatalf("inserts=%d records=%d commits=%d", m.inserts, len(m.records), len(reader.committed))
			}
		})
	}
}
func TestConsumerBatchBeforeDeadLetter(t *testing.T) {
	for _, failed := range []bool{false, true} {
		reader := &batchReader{messages: []kafkago.Message{batchMessage(1), {Topic: "clicks", Offset: 2, Value: []byte("{bad")}}}
		m := &testClickModel{}
		if failed {
			m.insertErr = errors.New("write failed")
		}
		writer := &testWriter{}
		reader.beforeCommit = func() {
			if m.inserts != 1 {
				t.Fatal("commit skipped pending batch")
			}
		}
		c := &Consumer{reader: reader, clickModel: m, deadLetter: writer, batchSize: 100, flushInterval: time.Second}
		err := c.Run(context.Background())
		if failed {
			if !errors.Is(err, m.insertErr) || len(reader.committed) != 0 || writer.calls != 0 {
				t.Fatal("failed batch was skipped")
			}
		} else if !errors.Is(err, context.Canceled) || len(reader.committed) != 2 || writer.calls != 1 {
			t.Fatalf("error=%v commits=%d sends=%d", err, len(reader.committed), writer.calls)
		}
	}
}

// stopReader 模拟先收到一条消息，再收到退出信号。
type stopReader struct {
	testReader
	cancel context.CancelFunc
}

func (r *stopReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if r.fetched {
		r.cancel()
		return kafkago.Message{}, ctx.Err()
	}
	return r.testReader.FetchMessage(ctx)
}

type shutdownModel struct {
	testClickModel
}

func (m *shutdownModel) BatchInsert(ctx context.Context, records []*model.Click) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("shutdown flush has no deadline")
	}
	return m.testClickModel.BatchInsert(ctx, records)
}
func TestConsumerShutdown(t *testing.T) {
	for _, fail := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &stopReader{testReader: testReader{message: batchMessage(1)}, cancel: cancel}
		m := &shutdownModel{}
		if fail {
			m.insertErr = errors.New("write failed")
		}
		c := &Consumer{reader: reader, clickModel: m, batchSize: 100, flushInterval: time.Hour}
		err := c.Run(ctx)
		cancel()
		if !errors.Is(err, m.insertErr) {
			t.Fatalf("error=%v want=%v", err, m.insertErr)
		}
		if m.inserts != 1 {
			t.Fatalf("shutdown inserts=%d", m.inserts)
		}
		wantCommits := 1
		if fail {
			wantCommits = 0
		}
		if reader.commits != wantCommits {
			t.Fatalf("commits=%d want=%d", reader.commits, wantCommits)
		}
	}
}

type cancelCommitReader struct {
	testReader
	cancel context.CancelFunc
}

func (r *cancelCommitReader) CommitMessages(ctx context.Context, messages ...kafkago.Message) error {
	r.commits++
	if r.commits == 1 {
		r.cancel()
		return ctx.Err()
	}
	return ctx.Err()
}
func TestConsumerShutdownAfterInsert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &cancelCommitReader{testReader: testReader{message: batchMessage(1)}, cancel: cancel}
	m := &testClickModel{}
	c := &Consumer{reader: reader, clickModel: m, batchSize: 1, flushInterval: time.Second}
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if m.inserts != 1 || reader.commits != 2 {
		t.Fatalf("inserts=%d commits=%d", m.inserts, reader.commits)
	}
}
