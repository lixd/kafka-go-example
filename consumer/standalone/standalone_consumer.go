package standalone

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"kafka-go-example/conf"
)

/*
	本例展示最简单的 独立消费者 的使用（除独立消费者外 kafka 还有消费者组）
	本例中只包含了消息的消费逻辑，没有ACK相关逻辑,即所有消息都可以重复消费。

	注:kafka 中使用的是 offset 机制，每条消息都有一个 offset(类似于消息ID)，每个消费者会维护自己的消费 offset，
	kafka 中通过消费者 offset 和消息 offset 来区分哪些消息已消费，哪些没有。没有使用其他MQ的ACK机制。
	其他MQ中消息ACK后就会被删除，kafka 则不会，kafka 消息过期后才会删除，且过期时间可以自定义，
	即就算消费者A把所有消息都消费了，也可以重置自己的 offset 然后再从头开始消费。
	所以甚至可以将 kafka 用于存储消息。

名词: standalone consumer、topic、partition、offset
*/

// SinglePartition 单分区消费
func SinglePartition(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewConsumer err: ", err)
	}
	defer consumer.Close()
	// 参数1 指定消费哪个 topic
	// 参数2 分区 这里默认消费 0 号分区 kafka 中有分区的概念，类似于ES和MongoDB中的sharding，MySQL中的分表这种
	// 参数3 offset 从哪儿开始消费起走，正常情况下每次消费完都会将这次的offset提交到kafka，然后下次可以接着消费，
	// 这里demo就从最新的开始消费，即该 consumer 启动之前产生的消息都无法被消费
	// 如果改为 sarama.OffsetOldest 则会从最旧的消息开始消费，即每次重启 consumer 都会把该 topic 下的所有消息消费一次
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("ConsumePartition err: ", err)
	}
	defer partitionConsumer.Close()
	// 会一直阻塞在这里
	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}

// Partitions 多分区消费
func Partitions(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewConsumer err: ", err)
	}
	defer consumer.Close()
	// 先查询该 topic 有多少分区
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal("Partitions err: ", err)
	}
	var wg sync.WaitGroup
	// 然后每个分区开一个 goroutine 来消费
	for _, partitionId := range partitions {
		go consumeByPartition(consumer, partitionId, &wg)
	}
	wg.Wait()
}

func consumeByPartition(consumer sarama.Consumer, partitionId int32, wg *sync.WaitGroup) {
	defer wg.Done()
	partitionConsumer, err := consumer.ConsumePartition(conf.Topic, partitionId, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("ConsumePartition err: ", err)
	}
	defer partitionConsumer.Close()
	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}
