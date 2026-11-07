package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

// defaultBatchTimeout 未攒满批次时的最长刷新间隔，kafka-go 默认 1s，过大会增加投递延迟
const defaultBatchTimeout = 10 * time.Millisecond

// Producer 基于 kafka-go 的异步点击事件生产者
type Producer struct {
	writer *kafkago.Writer
}

var _ ClickProducer = (*Producer)(nil)

// NewProducer 创建异步生产者；brokers 或 topic 为空时返回错误
func NewProducer(brokers []string, topic string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka: brokers must not be empty")
	}
	if topic == "" {
		return nil, errors.New("kafka: topic must not be empty")
	}

	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{}, // 同一短链落到同一分区，保证顺序
			Async:        true,            // 异步写入，不阻塞重定向主流程
			BatchTimeout: defaultBatchTimeout,
			Completion: func(msgs []kafkago.Message, err error) {
				if err != nil {
					logx.Errorf("kafka: send click events failed, topic=%s, count=%d, err=%v", topic, len(msgs), err)
				}
			},
		},
	}, nil
}

// Send 异步投递点击事件；仅返回序列化/参数校验错误，投递失败由 Completion 回调记录
func (p *Producer) Send(ctx context.Context, event ClickEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(event.ShortURL),
		Value: value,
	})
}

// Close 刷新缓冲区并关闭 writer
func (p *Producer) Close() error {
	return p.writer.Close()
}
