// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"shorturl/internal/config"
	"shorturl/model"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config        config.Config
	ShortUrlModel model.ShortUrlMapModel
	SequenceModel model.SequenceModel
}

func NewServiceContext(c config.Config) *ServiceContext {
	sUrlConn := sqlx.NewMysql(c.ShortUrlDB.DSN)
	seqConn := sqlx.NewMysql(c.SequenceDB.DSN)

	return &ServiceContext{
		Config:        c,
		ShortUrlModel: model.NewShortUrlMapModel(sUrlConn),
		SequenceModel: model.NewSequenceModel(seqConn),
	}
}
