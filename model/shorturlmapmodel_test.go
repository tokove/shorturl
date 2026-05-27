package model

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"golang.org/x/sync/singleflight"
)

func TestFindLurlBySurlConcurrent(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.New(mr.Addr())
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	conn := sqlx.NewSqlConnFromDB(db)
	defer db.Close()
	sf := new(singleflight.Group)

	// 设置期望的 SQL 查询
	mock.ExpectQuery("select .* from `short_url_map`").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "create_at", "create_by", "is_del", "lurl", "md5", "surl"}).
			AddRow(1, time.Now(), "", 0, "https://github.com/", "abc", "k"))

	m := NewShortUrlMapModel(conn, rdb, sf)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lurl, err := m.FindLurlBySurl(context.Background(), "k")
			t.Logf("lurl: %s, err: %v", lurl, err)
		}()
	}
	wg.Wait()

	// 验证 SQL 只执行了一次
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
