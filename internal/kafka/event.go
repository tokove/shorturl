package kafka

import (
	"github.com/google/uuid"
	"time"
)

// ClickEvent 短链点击事件
type ClickEvent struct {
	EventID  string `json:"event_id"`
	ShortURL string `json:"short_url"`
	Ts       int64  `json:"ts"` // 点击时间，Unix 毫秒时间戳
}

// NewClickEvent 以当前时间创建一条点击事件
func NewClickEvent(shortURL string) ClickEvent {
	return ClickEvent{
		EventID:  uuid.NewString(),
		ShortURL: shortURL,
		Ts:       time.Now().UnixMilli(),
	}
}
