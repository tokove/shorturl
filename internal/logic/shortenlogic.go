// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"shorturl/internal/svc"
	"shorturl/internal/types"
	"shorturl/model"
	"shorturl/pkg/base62"
	"shorturl/pkg/connect"
	"shorturl/pkg/md5"
	"shorturl/pkg/urltool"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ShortenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewShortenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShortenLogic {
	return &ShortenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ShortenLogic) Shorten(req *types.ShortenRequest) (resp *types.ShortenResponse, err error) {
	// 1. 参数校验
	// 1.1 不能为空：handler层已经做了校验，这里可以不再校验
	// 1.2 长链合法
	if ok := connect.Get(req.LongURL); !ok {
		return nil, errors.New("invalid long url")
	}
	// 1.3 是否转链过
	// 1.3.1 将长链转为MD5
	md5Str := md5.Sum([]byte(req.LongURL))
	// 1.3.2 从数据库中查询MD5对应的短链
	u, err := l.svcCtx.ShortUrlModel.FindOneByMd5(l.ctx, sql.NullString{String: md5Str, Valid: true})
	if !errors.Is(err, sqlx.ErrNotFound) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("long url has been shortened before, short url: %s", u.Surl.String)
	}
	// 1.4 不能是一个短链接
	// https://www.google.com/path/?code=123， 取出path，判断是否是一个短链接
	baseUrl, err := urltool.GetBasePath(req.LongURL)
	if err != nil {
		return nil, err
	}
	_, err = l.svcCtx.ShortUrlModel.FindOneBySurl(l.ctx, sql.NullString{String: baseUrl, Valid: true})
	if !errors.Is(err, sqlx.ErrNotFound) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("long url is already a short url, short url: %s", baseUrl)
	}
	// 2. 取号生成短链
	var seq uint64
	var shorturl string
	for {
		seq, err = l.svcCtx.Sequence.Next()
		if err != nil {
			return nil, err
		}
		shorturl = base62.Int2String(seq)
		// 2.1 安全性 base62串是乱序的
		// 2.2 违禁词
		if _, ok := l.svcCtx.ShortUrlBlackList[strings.ToLower(shorturl)]; !ok {
			break
		}
	}
	// 3. 存储短链和长链的映射关系
	_, err = l.svcCtx.ShortUrlModel.Insert(l.ctx, &model.ShortUrlMap{
		Lurl: sql.NullString{String: req.LongURL, Valid: true},
		Surl: sql.NullString{String: shorturl, Valid: true},
		Md5:  sql.NullString{String: md5Str, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	// 4. 加入到布隆过滤器中
	if err := l.svcCtx.Filter.Add([]byte(shorturl)); err != nil {
		return nil, err
	}
	// 5. 返回短链
	return &types.ShortenResponse{
		ShortURL: l.svcCtx.Config.ShortDomain + "/" + shorturl,
	}, nil
}
