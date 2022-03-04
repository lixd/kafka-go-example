package main

import (
	"kafka-go-example/conf"
	"kafka-go-example/consumer/group"
)

// 本例展示最简单的 消费者组 的使用
/*
topic 有多个分区时，消息会自动路由到对应的分区,因为路由算法的关系 可能不会平均分.
kafka 会以分区为单位（一个分区只能被一个 consumer 消费.），让消费者组中的多个消费者消费同一个 topic 下的消息（）
如果该 topic 只有一个分区则只会有一个 consumer 能够取到消息
*/
func main() {
	// 测试: 该topic创建时只指定两个分区，然后启动3个消费者进行消费，
	// 结果: 最终只有两个消费者能够消费到消息
	topic := conf.Topic2
	go group.ConsumerGroup(topic, conf.ConsumerGroupID, "C1")
	go group.ConsumerGroup(topic, conf.ConsumerGroupID, "C2")
	group.ConsumerGroup(topic, conf.ConsumerGroupID, "C3")
}
