// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"shorturl/internal/config"
	"shorturl/model"
	"shorturl/sequence"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config            config.Config
	ShortUrlModel     model.ShortUrlMapModel
	Sequence          sequence.Sequence
	ShortUrlBlackList map[string]struct{}
	CacheRedis        *redis.Redis
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := sqlx.NewMysql(c.ShortUrlDB.DSN)
	m := make(map[string]struct{}, len(c.ShortUrlBlackList))
	for _, word := range c.ShortUrlBlackList {
		m[word] = struct{}{}
	}
	cacherdb := redis.New(c.CacheRedis.Addr)
	return &ServiceContext{
		Config:        c,
		ShortUrlModel: model.NewShortUrlMapModel(conn, cacherdb),
		Sequence:      sequence.NewMysql(c.SequenceDB.DSN),
		// Sequence:          sequence.NewRedis(c.SequenceRedis.Addr),
		ShortUrlBlackList: m,
		CacheRedis:        cacherdb,
	}
}
