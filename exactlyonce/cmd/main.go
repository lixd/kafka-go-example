package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
	"kafka-go-example/exactlyonce"
)

func main() {
	topic := conf.Topic
	// 先启动消费者,保证能消费到后续发送的消息
	go standalone.SinglePartition(topic)
	time.Sleep(time.Second)
	exactlyonce.Producer(topic, 10)
	// sleep 等待消费结束
	time.Sleep(time.Second * 10)
}
