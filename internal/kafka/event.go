package kafka

import (
	"errors"
	"time"

	"github.com/google/uuid"
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

func (e ClickEvent) validate() error {
	id, err := uuid.Parse(e.EventID)
	if err != nil || id == uuid.Nil || id.String() != e.EventID {
		return errors.New("event_id must be a nonzero canonical UUID")
	}
	if len(e.ShortURL) == 0 || len(e.ShortURL) > 11 {
		return errors.New("short_url must contain 1 to 11 Base62 characters")
	}
	for _, c := range e.ShortURL {
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return errors.New("short_url must contain only Base62 characters")
		}
	}
	if e.Ts <= 0 {
		return errors.New("ts must be a positive Unix millisecond timestamp")
	}
	return nil
}
