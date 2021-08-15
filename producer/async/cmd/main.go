package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
	"kafka-go-example/producer/async"
)

func main() {
	topic := conf.Topic
	go standalone.SinglePartition(topic)
	time.Sleep(time.Millisecond * 100) // 延迟，让consumer启动后再启动生产者
	async.Producer(topic, 100)

	time.Sleep(time.Second * 10)
}
