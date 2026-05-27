// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package config

import "github.com/zeromicro/go-zero/rest"

type Config struct {
	rest.RestConf
	SequenceDB struct {
		DSN string
	}
	ShortUrlDB struct {
		DSN string
	}
	SequenceRedis struct {
		Addr string
	}
	CacheRedis struct {
		Addr string
	}
	ShortUrlBlackList []string
	ShortDomain       string
}
