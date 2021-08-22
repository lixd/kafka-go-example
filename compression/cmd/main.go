package main

import (
	"time"

	"kafka-go-example/compression"
	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
)

func main() {
	topic := conf.TopicCompression
	// 先启动消费者,保证能消费到后续发送的消息
	go standalone.SinglePartition(topic)
	time.Sleep(time.Second)
	compression.Producer(topic, 1000)
	// sleep 等待消费结束
	time.Sleep(time.Second * 10)
}
