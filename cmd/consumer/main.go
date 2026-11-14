package main

import (
	"context"
	"flag"
	"log"
	"time"

	"shorturl/internal/config"
	"shorturl/internal/kafka"
	"shorturl/model"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var configFile = flag.String("f", "etc/shorturl-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	if c.Kafka.DeadLetterTopic == "" || c.Kafka.DeadLetterTopic == c.Kafka.Topic {
		log.Fatal("Kafka.DeadLetterTopic must be nonempty and different from Kafka.Topic")
	}
	conn := sqlx.NewMysql(c.ClickDB.DSN)
	clickModel := model.NewClickModel(conn)
	consumer := kafka.NewConsumer(c.Kafka.Brokers, c.Kafka.Topic, c.Kafka.GroupID, c.Kafka.DeadLetterTopic, c.Kafka.BatchSize, c.Kafka.FlushInterval, clickModel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Kafka 关闭连接可能需要等待正在进行的网络请求结束。
	proc.SetTimeToForceQuit(30 * time.Second)
	proc.AddWrapUpListener(cancel)

	err := consumer.Run(ctx)
	if closeErr := consumer.Close(); closeErr != nil {
		log.Print(closeErr)
	}
	if err != nil {
		log.Fatal(err)
	}
}
