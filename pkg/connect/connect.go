package connect

import (
	"net/http"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// client 通用客户端
var client = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
	Timeout: 2 * time.Second,
}

// Get 发送一个 GET 请求，判断 URL 是否可访问
func Get(url string) bool {
	resp, err := client.Get(url)
	if err != nil {
		logx.Errorw("http get error", logx.LogField{Key: "error", Value: err.Error()})
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
