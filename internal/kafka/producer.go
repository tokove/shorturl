package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"shorturl/model"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

// defaultBatchTimeout 未攒满批次时的最长刷新间隔，kafka-go 默认 1s，过大会增加投递延迟
const defaultBatchTimeout = 10 * time.Millisecond

// Producer 基于 kafka-go 的异步点击事件生产者
type Producer struct {
	writer     messageWriter
	clickModel model.ClickModel
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
	p := &Producer{clickModel: clickModel}
	p.writer = &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{}, // 同一短链落到同一分区，保证顺序
		Async:        true,            // 异步写入，不阻塞重定向主流程
		BatchTimeout: defaultBatchTimeout,
		RequiredAcks: kafkago.RequireAll,
		Completion: func(msgs []kafkago.Message, err error) {
			if err != nil {
				logx.Errorf("kafka: send click events failed, saving to mysql, topic=%s, count=%d, err=%v", topic, len(msgs), err)
				if saveErr := p.save(msgs); saveErr != nil {
					logx.Errorf("kafka: save failed click events failed, topic=%s, count=%d, err=%v, messages=%s", topic, len(msgs), saveErr, messageValues(msgs))
				}
			}
		},
	}
	return p, nil
}

// Send 异步投递点击事件；发送失败则补写 MySQL，补写成功返回 nil。
// 发送失败时补写 MySQL，入队后的失败由 Completion 回调处理。
func (p *Producer) Send(ctx context.Context, event ClickEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafkago.Message{
		Key:   []byte(event.ShortURL),
		Value: value,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		if saveErr := p.save([]kafkago.Message{msg}); saveErr != nil {
			logx.Errorf("kafka: send and save click event failed, event=%s, err=%v", value, saveErr)
			return errors.Join(err, saveErr)
		}
	}
	return nil
}

// Close 刷新缓冲区并关闭 writer
func (p *Producer) Close() error {
	return p.writer.Close()
}

// save 使用独立超时，避免请求结束后取消补写；保留 event_id 以便消费端去重。
func (p *Producer) save(messages []kafkago.Message) error {
	records := make([]*model.Click, 0, len(messages))
	for _, msg := range messages {
		var event ClickEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("decode failed click event: %w", err)
		}
		records = append(records, &model.Click{EventId: event.EventID, Surl: event.ShortURL, Ts: event.Ts})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	return retry(ctx, func() error { return p.clickModel.BatchInsert(ctx, records) }, isRetryableDBError)
}

func messageValues(messages []kafkago.Message) []string {
	values := make([]string, 0, len(messages))
	for _, msg := range messages {
		values = append(values, string(msg.Value))
	}
	return values
}
