// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"

	"shorturl/internal/kafka"
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
	// 1. 查询布隆过滤器
	exists, err := l.svcCtx.Filter.Exists([]byte(req.ShortURL))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("short url not found")
	}
	// 2. 根据短链查长链
	u, err := l.svcCtx.ShortUrlModel.FindLurlBySurl(l.ctx, req.ShortURL)
	if err != nil {
		return nil, err
	}
	// 3. 发送点击事件到kafka（旁路统计，失败不影响重定向）
	if err := l.svcCtx.Producer.Send(l.ctx, kafka.NewClickEvent(req.ShortURL)); err != nil {
		l.Errorf("send click event failed, surl=%s, err=%v", req.ShortURL, err)
	}
	// 4. 返回长链，进行重定向
	return &types.RedirectResponse{LongURL: u}, nil
}
