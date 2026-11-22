package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"shorturl/model"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultBatchTimeout  = 10 * time.Millisecond
	producerQueueSize    = 4096
	producerBatchSize    = 100
	producerCloseTimeout = 10 * time.Second
)

var (
	errProducerClosed = errors.New("kafka: click producer closed")
	errClickQueueFull = errors.New("kafka: click queue full")
)

// Producer 在后台批量发送点击事件，并补写发送失败的事件。
type Producer struct {
	writer     messageWriter
	clickModel model.ClickModel
	queue      chan ClickEvent
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan error

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

var _ ClickProducer = (*Producer)(nil)

// NewProducer 创建异步生产者；brokers 或 topic 为空时返回错误
func NewProducer(brokers []string, topic string, clickModel model.ClickModel) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka: brokers must not be empty")
	}
	if topic == "" {
		return nil, errors.New("kafka: topic must not be empty")
	}

	if clickModel == nil {
		return nil, errors.New("kafka: click model must not be nil")
	}
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchSize:    producerBatchSize,
		BatchTimeout: defaultBatchTimeout,
		WriteTimeout: 2 * time.Second,
		MaxAttempts:  3,
		RequiredAcks: kafkago.RequireAll,
	}
	return newProducer(writer, clickModel), nil
}

func newProducer(writer messageWriter, clickModel model.ClickModel) *Producer {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		writer:     writer,
		clickModel: clickModel,
		queue:      make(chan ClickEvent, producerQueueSize),
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan error, 1),
	}
	go p.run()
	return p
}

// Send 尝试将已发生的点击入队，后续处理独立于请求上下文。
func (p *Producer) Send(_ context.Context, event ClickEvent) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errProducerClosed
	}
	select {
	case p.queue <- event:
		return nil
	default:
		return errClickQueueFull
	}
}

// Close 停止接收点击，等待后台处理已入队的事件。
func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		defer p.cancel()
		p.mu.Lock()
		p.closed = true
		close(p.queue)
		p.mu.Unlock()

		timer := time.NewTimer(producerCloseTimeout)
		defer timer.Stop()
		select {
		case p.closeErr = <-p.done:
		case <-timer.C:
			p.closeErr = errors.New("kafka: click producer shutdown timed out")
		}
	})
	return p.closeErr
}

func (p *Producer) run() {
	defer func() { p.done <- p.writer.Close() }()
	events := make([]ClickEvent, 0, producerBatchSize)
	for first := range p.queue {
		if p.ctx.Err() != nil {
			return
		}
		events = append(events[:0], first)
		for len(events) < producerBatchSize && len(p.queue) > 0 {
			events = append(events, <-p.queue)
		}
		p.publish(events)
	}
}

func (p *Producer) publish(events []ClickEvent) {
	messages := make([]kafkago.Message, len(events))
	for i, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			logx.Errorf("kafka: encode click event failed, event_id=%s, err=%v", event.EventID, err)
			return
		}
		messages[i] = kafkago.Message{Key: []byte(event.ShortURL), Value: value}
	}
	err := p.writer.WriteMessages(p.ctx, messages...)
	if err == nil {
		return
	}
	failed := events
	var writeErrors kafkago.WriteErrors
	if errors.As(err, &writeErrors) && len(writeErrors) == len(events) {
		failed = events[:0]
		for i, writeErr := range writeErrors {
			if writeErr != nil {
				failed = append(failed, events[i])
			}
		}
	}
	if len(failed) == 0 {
		return
	}
	logx.Errorf("kafka: send click events failed, saving to mysql, count=%d, err=%v", len(failed), err)
	if saveErr := p.save(failed); saveErr != nil {
		logx.Errorf("kafka: save click events failed, events=%+v, err=%v", failed, saveErr)
	}
}

func (p *Producer) save(events []ClickEvent) error {
	records := make([]*model.Click, 0, len(events))
	for _, event := range events {
		records = append(records, &model.Click{EventId: event.EventID, Surl: event.ShortURL, Ts: event.Ts})
	}
	ctx, cancel := context.WithTimeout(p.ctx, 4*time.Second)
	defer cancel()
	return retry(ctx, func() error { return p.clickModel.BatchInsert(ctx, records) }, isRetryableDBError)
}
