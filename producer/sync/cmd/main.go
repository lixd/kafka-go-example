package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/producer/sync"
)

// 本例展示最简单的 同步生产者 的使用
func main() {
	sync.Producer(conf.Topic, 100)
}
