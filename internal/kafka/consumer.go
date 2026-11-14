package kafka

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"shorturl/model"

	"github.com/go-sql-driver/mysql"
	kafkago "github.com/segmentio/kafka-go"
)

type messageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
	Close() error
}

// Consumer 收集点击事件并批量写入 MySQL。
type Consumer struct {
	reader        messageReader
	deadLetter    messageWriter
	clickModel    model.ClickModel
	batchSize     int
	flushInterval time.Duration
}

func NewConsumer(brokers []string, topic, groupID, deadLetterTopic string, batchSize int, flushInterval time.Duration, clickModel model.ClickModel) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		deadLetter: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        deadLetterTopic,
			RequiredAcks: kafkago.RequireAll,
			WriteTimeout: 10 * time.Second,
		},
		clickModel:    clickModel,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

func (c *Consumer) Run(ctx context.Context) (err error) {
	if c.batchSize <= 0 || c.flushInterval <= 0 {
		return errors.New("batch size and flush interval must be positive")
	}
	records := make([]*model.Click, 0, c.batchSize)
	messages := make([]kafkago.Message, 0, c.batchSize)
	var deadline time.Time
	inserted := false
	flush := func(ctx context.Context) error {
		if len(records) == 0 {
			return nil
		}
		if !inserted {
			if err := retry(ctx, func() error {
				return c.clickModel.BatchInsert(ctx, records)
			}, isRetryableDBError); err != nil {
				return err
			}
			inserted = true
		}
		// 入库成功后只重试提交，不再重复写库。
		if err := c.commit(ctx, messages...); err != nil {
			return err
		}
		clear(records)
		clear(messages)
		records = records[:0]
		inserted = false
		messages = messages[:0]
		return nil
	}
	// 只在运行上下文被取消时收尾；普通消费错误仍直接返回。
	defer func() {
		if ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
			return
		}
		// 独立限制收尾时间，失败时不提交，留待下次消费重放。
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 4*time.Second)
		defer cancel()
		err = flush(shutdownCtx)
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(records) > 0 && !time.Now().Before(deadline) {
			if err := flush(ctx); err != nil {
				return err
			}
		}
		// 有待写数据时，拉取消息最多等到本批次的刷新时间。
		fetchCtx := ctx
		var cancel context.CancelFunc
		if len(records) > 0 {
			fetchCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		msg, err := c.reader.FetchMessage(fetchCtx)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && len(records) > 0 {
				if err := flush(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		var event ClickEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// 先处理前面的正常消息，避免坏消息的 offset 越过未入库数据。
			if flushErr := flush(ctx); flushErr != nil {
				return flushErr
			}
			if err := c.sendDeadLetter(ctx, msg, err); err != nil {
				return err
			}
			if err := c.commit(ctx, msg); err != nil {
				return err
			}
			continue
		}
		if len(records) == 0 {
			deadline = time.Now().Add(c.flushInterval)
		}
		records = append(records, &model.Click{EventId: event.EventID, Surl: event.ShortURL, Ts: event.Ts})
		messages = append(messages, msg)
		if len(records) >= c.batchSize {
			if err := flush(ctx); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) commit(ctx context.Context, messages ...kafkago.Message) error {
	return retry(ctx, func() error { return c.reader.CommitMessages(ctx, messages...) }, nil)
}

func (c *Consumer) sendDeadLetter(ctx context.Context, msg kafkago.Message, cause error) error {
	// 保留原消息内容，来源和解析错误放在 Header 中。
	headers := append([]kafkago.Header(nil), msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "source-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "source-partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafkago.Header{Key: "source-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafkago.Header{Key: "error", Value: []byte(cause.Error())},
	)
	return retry(ctx, func() error {
		return c.deadLetter.WriteMessages(ctx, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
	}, nil)
}

func (c *Consumer) Close() error {
	return errors.Join(c.reader.Close(), c.deadLetter.Close())
}

// retry 首次失败后最多重试 3 次，每次间隔 1 秒。
// retryable 为 nil 时重试所有非取消、非超时错误。
func retry(ctx context.Context, operation func() error, retryable func(error) bool) error {
	const maxRetries = 3
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil {
			return nil
		}
		if attempt >= maxRetries || errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(retryable != nil && !retryable(err)) {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func isRetryableDBError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1040, 1205, 1213, 2006, 2013: // 连接数耗尽、锁等待超时、死锁、连接断开。
			return true
		}
	}
	return false
}
