package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
	"kafka-go-example/producer/sync"
)

// 测试 独立消费者 先启动消费者再启动生产者
func main() {
	topic := conf.Topic
	go standalone.SinglePartition(topic)
	// go standalone.Partitions(topic)
	time.Sleep(time.Millisecond * 100)
	sync.Producer(topic, 100)
	time.Sleep(time.Second * 10)
}
