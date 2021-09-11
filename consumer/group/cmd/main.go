package main

import (
	"time"

	"kafka-go-example/conf"
	"kafka-go-example/consumer/group"
	"kafka-go-example/producer/async"
)

// 一个分区只能被一个 consumer 消费.
// 两个 consumer 共同消费一个 topic 的多个分区，如果只有一个分区则只有一个 consumer 能够取到消息
func main() {
	topic := conf.Topic2
	async.Producer(topic, 100)
	time.Sleep(time.Second)
	go group.ConsumerGroup(topic, conf.ConsumerGroupID, "CG1")
	go group.ConsumerGroup(topic, conf.ConsumerGroupID, "CG2")
	// go group.ConsumerGroup(topic, kafka.ConsumerGroupID, "CG3") // 该 topic 只有两个分区 如果启动 3 个消费者会导致其中有一个不会消费到任何消息
	// topic 有多个分区时，消息会自动路由到对应的分区,因为路由算法的关系 可能不会平均分
	time.Sleep(time.Second * 20)
}
