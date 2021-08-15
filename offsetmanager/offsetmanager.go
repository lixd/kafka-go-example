package offsetmanager

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"kafka-go-example/conf"
)

/*
	本例展示最简单的 偏移量管理器 的手动使用（在 消费者组中 sarama 库实现了偏移量自动管理）
	增加偏移量管理后就可以记录下每次消费的位置，便于下次接着消费，避免 sarama.OffsetOldest  的重复消费或者 sarama.OffsetNewest 漏掉部分消息
	NOTE: 相比普通consumer增加了OffsetManager，调用 MarkOffset 手动记录了当前消费的 offset，
	最后调用 commit 提交到 kafka。
	sarama 库的自动提交就相当于 offsetManager.Commit() 操作，还是需要手动调用 MarkOffset。
*/

func OffsetManager(topic string) {
	config := sarama.NewConfig()
	// 配置开启自动提交 offset，这样 samara 库会定时帮我们把最新的 offset 信息提交给 kafka
	config.Consumer.Offsets.AutoCommit.Enable = true              // 开启自动 commit offset
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second // 自动 commit时间间隔
	client, err := sarama.NewClient([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewClient err: ", err)
	}
	defer client.Close()
	// offsetManager 用于管理每个 consumerGroup的 offset
	// 根据 groupID 来区分不同的 consumer，注意: 每次提交的 offset 信息也是和 groupID 关联的
	offsetManager, err := sarama.NewOffsetManagerFromClient("myGroupID", client) // 偏移量管理器
	if err != nil {
		log.Println("NewOffsetManagerFromClient err:", err)
	}
	defer offsetManager.Close()
	// 每个分区的 offset 也是分别管理的，demo 这里使用 0 分区，因为该 topic 只有 1 个分区
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, conf.DefaultPartition) // 对应分区的偏移量管理器
	if err != nil {
		log.Println("ManagePartition err:", err)
	}
	defer partitionOffsetManager.Close()
	// defer 在程序结束后在 commit 一次，防止自动提交间隔之间的信息被丢掉
	defer offsetManager.Commit()
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Println("NewConsumerFromClient err:", err)
	}
	// 根据 kafka 中记录的上次消费的 offset 开始+1的位置接着消费
	nextOffset, _ := partitionOffsetManager.NextOffset() // 取得下一消息的偏移量作为本次消费的起点
	fmt.Println("nextOffset:", nextOffset)
	pc, err := consumer.ConsumePartition(topic, conf.DefaultPartition, nextOffset)
	if err != nil {
		log.Println("ConsumePartition err:", err)
	}
	defer pc.Close()

	for message := range pc.Messages() {
		value := string(message.Value)
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, value)
		// 每次消费后都更新一次 offset,这里更新的只是程序内存中的值，需要 commit 之后才能提交到 kafka
		partitionOffsetManager.MarkOffset(message.Offset+1, "modified metadata") // MarkOffset 更新最后消费的 offset
	}
}
