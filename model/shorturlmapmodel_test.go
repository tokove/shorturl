package model

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"golang.org/x/sync/singleflight"
)

type lookupContextConn struct {
	sqlx.SqlConn
	started chan context.Context
	stop    <-chan struct{}
}

func (c lookupContextConn) QueryRowCtx(ctx context.Context, _ any, _ string, _ ...any) error {
	c.started <- ctx
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stop:
		return sqlx.ErrNotFound
	}
}

func TestFindLurlBySurlBoundsSharedLookup(t *testing.T) {
	mr := miniredis.RunT(t)
	lc, err := ristretto.NewCache(&ristretto.Config{NumCounters: 100, MaxCost: 1024, BufferItems: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	stop := make(chan struct{})
	defer close(stop)
	conn := lookupContextConn{started: make(chan context.Context, 1), stop: stop}
	sf := new(singleflight.Group)
	m := NewShortUrlMapModel(conn, redis.New(mr.Addr()), sf, lc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callerDone := make(chan error, 1)
	go func() {
		_, err := m.FindLurlBySurl(ctx, "shared")
		callerDone <- err
	}()
	var lookupCtx context.Context
	select {
	case lookupCtx = <-conn.started:
	case err := <-callerDone:
		t.Fatalf("request returned before database lookup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("database lookup did not start")
	}
	deadline, ok := lookupCtx.Deadline()
	if !ok || time.Until(deadline) > 3*time.Second {
		t.Fatalf("shared lookup should have its own deadline within 3 seconds, got %v, %v", deadline, ok)
	}
	sharedDone := sf.DoChan("shared", func() (any, error) {
		return nil, errors.New("expected to join the active lookup")
	})
	cancel()
	if err := lookupCtx.Err(); err != nil {
		t.Fatalf("caller cancellation stopped the shared lookup: %v", err)
	}
	select {
	case result := <-sharedDone:
		if !errors.Is(result.Err, context.DeadlineExceeded) {
			t.Fatalf("shared lookup error = %v, want deadline exceeded", result.Err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("shared lookup did not finish at its deadline")
	}
}

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
	lc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000000,
		MaxCost:     104857600,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 设置期望的 SQL 查询
	mock.ExpectQuery("select .* from `short_url_map`").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "create_at", "create_by", "is_del", "lurl", "md5", "surl"}).
			AddRow(1, time.Now(), "", 0, "https://github.com/", "abc", "k"))

	m := NewShortUrlMapModel(conn, rdb, sf, lc)

	var wg sync.WaitGroup
	for range 1000 {
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
