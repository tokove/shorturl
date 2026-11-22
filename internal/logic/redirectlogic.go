// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"

	"shorturl/internal/kafka"
	"shorturl/internal/svc"
	"shorturl/internal/types"
	"shorturl/model"

	"github.com/zeromicro/go-zero/core/logx"
)

var ErrShortURLNotFound = errors.New("short url not found")

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
		return nil, ErrShortURLNotFound
	}
	// 2. 根据短链查长链
	u, err := l.svcCtx.ShortUrlModel.FindLurlBySurl(l.ctx, req.ShortURL)
	if errors.Is(err, model.ErrNotFound) {
		return nil, ErrShortURLNotFound
	}
	if err != nil {
		return nil, err
	}
	// 3. 发送点击事件到kafka（旁路统计，失败不影响重定向）
	event := kafka.NewClickEvent(req.ShortURL)
	if err := l.svcCtx.Producer.Send(l.ctx, event); err != nil {
		l.Errorf("enqueue click event failed, event_id=%s, surl=%s, err=%v", event.EventID, event.ShortURL, err)
	}
	// 4. 返回长链，进行重定向
	return &types.RedirectResponse{LongURL: u}, nil
}
