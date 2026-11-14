package model

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"regexp"
	"testing"
)

func TestBatchInsert(t *testing.T) {
	for _, failed := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		m := NewClickModel(sqlx.NewSqlConnFromDB(db))
		query := "insert into `click` (`surl`, `ts`, `event_id`) values (?, ?, ?),(?, ?, ?) on duplicate key update `id` = `id`"
		exp := mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("Ab1", int64(1), "same-id", "Ab2", int64(2), "new-id")
		writeErr := errors.New("write failed")
		if failed {
			exp.WillReturnError(writeErr)
		} else {
			exp.WillReturnResult(sqlmock.NewResult(1, 1))
		}
		err = m.BatchInsert(context.Background(), []*Click{{Surl: "Ab1", Ts: 1, EventId: "same-id"}, {Surl: "Ab2", Ts: 2, EventId: "new-id"}})
		if failed && !errors.Is(err, writeErr) || !failed && err != nil {
			t.Fatalf("error=%v", err)
		}
		if err := m.BatchInsert(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
