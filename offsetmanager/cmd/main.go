package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/offsetmanager"
	"kafka-go-example/producer/async"
)

func main() {
	topic := conf.Topic
	go offsetmanager.OffsetManager(topic)
	time.Sleep(time.Second) // sleep 让 consumer 先启动
	async.Producer(topic, 100)
	time.Sleep(time.Second * 10)
}
