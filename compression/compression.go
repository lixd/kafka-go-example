package compression

import (
	"log"
	"strings"

	"kafka-go-example/conf"

	"github.com/Shopify/sarama"
)

var defaultMsg = strings.Repeat("Golang", 1000)

func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	// 指定压缩算法和压缩等级
	// config.Producer.Compression = sarama.CompressionGZIP
	// config.Producer.CompressionLevel = gzip.BestCompression
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true // 这个默认值就是 true 可以不用手动 赋值

	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	defer producer.Close()
	for i := 0; i < limit; i++ {
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(defaultMsg)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d\n", partition, offset)
	}
}
