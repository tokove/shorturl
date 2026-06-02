package model

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dgraph-io/ristretto"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"golang.org/x/sync/singleflight"
)

var _ ShortUrlMapModel = (*customShortUrlMapModel)(nil)

type (
	// ShortUrlMapModel is an interface to be customized, add more methods here,
	// and implement the added methods in customShortUrlMapModel.
	ShortUrlMapModel interface {
		shortUrlMapModel
		withSession(session sqlx.Session) ShortUrlMapModel
		FindLurlBySurl(ctx context.Context, surl string) (string, error)
		FindSurlsByCursor(ctx context.Context, lastId uint64, limit uint64) ([]*ShortUrlMap, error)
	}

	customShortUrlMapModel struct {
		*defaultShortUrlMapModel
		rdb *redis.Redis
		sf  *singleflight.Group
		lc  *ristretto.Cache
	}
)

// NewShortUrlMapModel returns a model for the database table.
func NewShortUrlMapModel(conn sqlx.SqlConn, rdb *redis.Redis, sf *singleflight.Group, lc *ristretto.Cache) ShortUrlMapModel {
	return &customShortUrlMapModel{
		defaultShortUrlMapModel: newShortUrlMapModel(conn),
		rdb:                     rdb,
		sf:                      sf,
		lc:                      lc,
	}
}

func (m *customShortUrlMapModel) withSession(session sqlx.Session) ShortUrlMapModel {
	return NewShortUrlMapModel(sqlx.NewSqlConnFromSession(session), m.rdb, m.sf, m.lc)
}

const cacheKeyPrefix = "shorturl:cache:"
const cacheTTL = 60 * 60 * 24

func (m *customShortUrlMapModel) FindLurlBySurl(ctx context.Context, surl string) (string, error) {
	// 0. 先从本地缓存中查询
	if v, found := m.lc.Get(cacheKeyPrefix + surl); found {
		logx.Infof("Found long URL in local cache for short URL: %s", v.(string))
		return v.(string), nil
	}
	v, err, _ := m.sf.Do(surl, func() (any, error) {
		// 1. 从缓存中查询长链
		lurl, err := m.rdb.GetCtx(ctx, cacheKeyPrefix+surl)
		// 1.1 查到直接返回
		if err == nil && lurl != "" {
			logx.Infof("Found long URL in cache for short URL: %s", lurl)
			m.lc.SetWithTTL(cacheKeyPrefix+surl, lurl, 1, cacheTTL)
			return lurl, nil
		}
		// 1.2 没有查到，继续从数据库中查询
		// 2. 从数据库中查询长链
		u, err := m.FindOneBySurl(ctx, sql.NullString{String: surl, Valid: true})
		// 2.1 没有查到，返回错误
		if err != nil {
			return "", err
		}
		// 2.2 查到，写入缓存，并返回
		if ok := m.lc.SetWithTTL(cacheKeyPrefix+surl, u.Lurl.String, 1, cacheTTL); !ok {
			logx.Infof("Failed to set local cache for short URL: %s", surl)
		}
		err = m.rdb.SetexCtx(ctx, cacheKeyPrefix+surl, u.Lurl.String, cacheTTL)
		if err != nil {
			return "", err
		}
		return u.Lurl.String, nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (m *customShortUrlMapModel) FindSurlsByCursor(ctx context.Context, lastId uint64, limit uint64) ([]*ShortUrlMap, error) {
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE id > ? ORDER BY id LIMIT ?`, shortUrlMapRows, m.table)
	var resp []*ShortUrlMap
	if err := m.conn.QueryRowsCtx(ctx, &resp, query, lastId, limit); err != nil {
		return nil, err
	}
	return resp, nil
}
