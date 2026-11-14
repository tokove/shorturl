package model

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"strings"
)

var _ ClickModel = (*customClickModel)(nil)

type (
	// ClickModel is an interface to be customized, add more methods here,
	// and implement the added methods in customClickModel.
	ClickModel interface {
		clickModel
		BatchInsert(ctx context.Context, records []*Click) error
		withSession(session sqlx.Session) ClickModel
	}

	customClickModel struct {
		*defaultClickModel
	}
)

// NewClickModel returns a model for the database table.
func NewClickModel(conn sqlx.SqlConn) ClickModel {
	return &customClickModel{
		defaultClickModel: newClickModel(conn),
	}
}

func (m *customClickModel) withSession(session sqlx.Session) ClickModel {
	return NewClickModel(sqlx.NewSqlConnFromSession(session))
}

// BatchInsert 按 event_id 去重，重复事件保留原记录。
func (m *customClickModel) BatchInsert(ctx context.Context, records []*Click) error {
	if len(records) == 0 {
		return nil
	}
	values := make([]string, 0, len(records))
	args := make([]any, 0, len(records)*3)
	for _, record := range records {
		values = append(values, "(?, ?, ?)")
		args = append(args, record.Surl, record.Ts, record.EventId)
	}
	query := fmt.Sprintf("insert into %s (`surl`, `ts`, `event_id`) values %s on duplicate key update `id` = `id`", m.table, strings.Join(values, ","))
	_, err := m.conn.ExecCtx(ctx, query, args...)
	return err
}
