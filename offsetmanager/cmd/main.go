package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/offsetmanager"
)

// 本例展示最简单的 偏移量管理器 的手动使用
func main() {
	offsetmanager.OffsetManager(conf.Topic)
}
