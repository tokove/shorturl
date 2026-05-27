package filter

import (
	"github.com/zeromicro/go-zero/core/bloom"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type RedisFilter struct {
	filter *bloom.Filter
}

func NewRedisFilter(rdb *redis.Redis, key string, size uint) *RedisFilter {
	filter := bloom.New(rdb, key, size)
	return &RedisFilter{filter: filter}
}

func (f *RedisFilter) Exists(data []byte) (bool, error) {
	return f.filter.Exists(data)
}

func (f *RedisFilter) Add(data []byte) error {
	return f.filter.Add(data)
}
