package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
	"kafka-go-example/partition"
)

func main() {
	topic := conf.TopicPartition
	// 先启动消费者,保证能消费到后续发送的消息
	go standalone.SinglePartition(topic)
	time.Sleep(time.Second)
	partition.Producer(topic, 100)
	// sleep 等待消费结束
	time.Sleep(time.Second * 10)
}
