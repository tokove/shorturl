package kafka

import "context"

// ClickProducer 点击事件生产者，负责将短链点击事件投递到消息队列
type ClickProducer interface {
	// Send 投递一条点击事件
	Send(ctx context.Context, event ClickEvent) error
	// Close 刷新缓冲区中未发送的消息并释放资源
	Close() error
}
