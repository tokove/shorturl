// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"context"
	"shorturl/internal/config"
	"shorturl/internal/filter"
	"shorturl/model"
	"shorturl/sequence"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"golang.org/x/sync/singleflight"
)

type ServiceContext struct {
	Config            config.Config
	ShortUrlModel     model.ShortUrlMapModel
	Sequence          sequence.Sequence
	ShortUrlBlackList map[string]struct{}
	CacheRedis        *redis.Redis
	Filter            filter.Filter
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := sqlx.NewMysql(c.ShortUrlDB.DSN)
	m := make(map[string]struct{}, len(c.ShortUrlBlackList))
	for _, word := range c.ShortUrlBlackList {
		m[word] = struct{}{}
	}
	cacherdb := redis.New(c.CacheRedis.Addr)
	sf := new(singleflight.Group)
	// bloomrdb := redis.New(c.BloomFilter.Addr) // rdb版本
	svcCtx := &ServiceContext{
		Config:        c,
		ShortUrlModel: model.NewShortUrlMapModel(conn, cacherdb, sf),
		Sequence:      sequence.NewMysql(c.SequenceDB.DSN),
		// Sequence:          sequence.NewRedis(c.SequenceRedis.Addr),
		ShortUrlBlackList: m,
		CacheRedis:        cacherdb,
		// Filter:            filter.NewRedisFilter(bloomrdb, c.BloomFilter.Key, c.BloomFilter.Size), // rdb版本
		Filter: filter.NewMemoryFilter(c.BloomFilter.Size, c.BloomFilter.Percent), // 内存版本
	}
	if err := loadDataToFilter(svcCtx); err != nil {
		panic(err)
	}
	return svcCtx
}

// loadDataToFilter 从数据库中加载数据到布隆过滤器中，用于内存版本
func loadDataToFilter(svcCtx *ServiceContext) error {
	var (
		lastId  uint64 = 0
		limit   uint64 = 10000
		hasMore        = true
	)

	ctx := context.Background()
	for hasMore {
		surls, err := svcCtx.ShortUrlModel.FindSurlsByCursor(ctx, lastId, limit+1)
		if err != nil {
			return err
		}

		if len(surls) == 0 {
			break
		}
		if len(surls) <= int(limit) {
			hasMore = false
		} else {
			surls = surls[:limit]
		}

		for _, surl := range surls {
			if err := svcCtx.Filter.Add([]byte(surl.Surl.String)); err != nil {
				return err
			}
		}
		lastId = surls[len(surls)-1].Id
	}

	return nil
}
