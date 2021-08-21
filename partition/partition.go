package partition

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"kafka-go-example/conf"
)

type myPartitioner struct {
	partition int32
}

func NewMyPartitioner(topic string) sarama.Partitioner {
	return &myPartitioner{}
}

// Partition 返回的是分区的位置或者索引，并不是具体的分区号。比如有十个分区[0,1，2,3...9] 这里返回 0 表示取数组中的第0个位置的分区。在 Go 客户端中是这样实现的，具体见下文源码分析
func (p *myPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	// 简单轮询策略
	if p.partition >= numPartitions {
		p.partition = 0
	}
	fmt.Printf("msg:%s partition:%v \n", message.Value, p.partition)
	ret := p.partition
	p.partition++
	return ret, nil
}

// RequiresConsistency key->partition 的映射是否需要一致,如果强制指定需要一致,那么就算这个分区不可用了也会把消息发给该分区以保证一致性，未指定则只会把消息投递给可用分区。
// 当使用 Key-Ordering 策略的时候需要设置为 true 才能保证同一个 Key 被投递到同一个分区.
func (p *myPartitioner) RequiresConsistency() bool {
	return false
}

func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = NewMyPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true // 这个默认值就是 true 可以不用手动 赋值

	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	defer producer.Close()
	for i := 0; i < limit; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(str)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
}
