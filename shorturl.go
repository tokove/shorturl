// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"flag"
	"fmt"
	"time"

	"shorturl/internal/config"
	"shorturl/internal/handler"
	"shorturl/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/shorturl-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()
	proc.SetTimeToForceQuit(15 * time.Second)

	ctx := svc.NewServiceContext(c)
	// HTTP 服务停止后，限时处理队列中的点击事件。
	defer func() {
		if err := ctx.Producer.Close(); err != nil {
			logx.Errorf("close kafka producer failed: %v", err)
		}
	}()
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
