package consumer

import (
	"log"

	"github.com/Shopify/sarama"
	"kafka-go-example/conf"
)

// Consume 消费者
func Consume(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewConsumer err: ", err)
	}
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("ConsumePartition err: ", err)
	}
	defer partitionConsumer.Close()
	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}
