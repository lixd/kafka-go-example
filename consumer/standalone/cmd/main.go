package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/consumer/standalone"
)

// 本例展示最简单的 独立消费者 的使用
func main() {
	standalone.SinglePartition(conf.Topic)
}
