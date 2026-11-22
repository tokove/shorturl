package kafka

import "context"

// ClickProducer 点击事件生产者，负责将短链点击事件投递到消息队列
type ClickProducer interface {
	// Send 将点击事件交给后台处理，队列满或关闭时返回错误。
	Send(ctx context.Context, event ClickEvent) error
	// Close 刷新缓冲区中未发送的消息并释放资源
	Close() error
}
