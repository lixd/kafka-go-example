package producer

import (
	"log"
	"strconv"
	"time"

	"kafka-go-example/conf"

	"github.com/Shopify/sarama"
)

func Produce(topic string, limit int) {
	config := sarama.NewConfig()
	// 因为同步生产者在发送之后就必须返回状态，所以需要两个都返回
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
		partition, offset, err := producer.SendMessage(msg) // 发送逻辑也是封装的异步发送逻辑，可以理解为将异步封装成了同步
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
}
