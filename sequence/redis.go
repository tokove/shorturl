package sequence

import "github.com/zeromicro/go-zero/core/stores/redis"

type Redis struct {
	redis *redis.Redis
}

func NewRedis(addr string) *Redis {
	return &Redis{redis: redis.New(addr)}
}

const redisSequenceKey = "shorturl:sequence"

func (r *Redis) Next() (uint64, error) {
	lid, err := r.redis.Incr(redisSequenceKey)
	if err != nil {
		return 0, err
	}
	return uint64(lid), nil
}
