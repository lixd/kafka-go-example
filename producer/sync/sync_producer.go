package sync

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"kafka-go-example/conf"
)

/*
	本例展示最简单的 同步生产者 的使用（除同步生产者外 kafka 还有异步生产者）
	名词 sync producer
*/

func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	// 同步生产者必须同时开启 Return.Successes 和 Return.Errors
	// 因为同步生产者在发送之后就必须返回状态，所以需要两个都返回
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true // 这个默认值就是 true 可以不用手动 赋值
	// 同步生产者和异步生产者逻辑是一致的，Success或者Errors都是通过channel返回的，
	// 只是同步生产者封装了一层，等channel返回之后才返回给调用者
	// 具体见 sync_producer.go 文件72行 newSyncProducerFromAsyncProducer 方法
	// 内部启动了两个 goroutine 分别处理Success Channel 和 Errors Channel
	// 同步生产者内部就是封装的异步生产者
	// type syncProducer struct {
	// 	producer *asyncProducer
	// 	wg       sync.WaitGroup
	// }
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
