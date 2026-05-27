// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"shorturl/internal/svc"
	"shorturl/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type RedirectLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRedirectLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RedirectLogic {
	return &RedirectLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RedirectLogic) Redirect(req *types.RedirectRequest) (resp *types.RedirectResponse, err error) {
	// 1. 根据短链查长链
	u, err := l.svcCtx.ShortUrlModel.FindLurlBySurl(l.ctx, req.ShortURL)
	if err != nil {
		return nil, err
	}
	// 2. 返回长链，进行重定向
	return &types.RedirectResponse{LongURL: u}, nil
}
