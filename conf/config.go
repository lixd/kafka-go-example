package conf

// 相关配置信息
const (
	HOST = "123.57.236.125:9092"
	// Topic 注: 如果关闭了自动创建分区，使用前都需要手动创建对应分区
	Topic            = "standAlone"
	Topic2           = "consumerGroup"
	Topic3           = "benchmark"
	TopicPartition   = "partition"
	TopicCompression = "compression"
	DefaultPartition = 0
	ConsumerGroupID  = "cg1"
	ConsumerGroupID2 = "cg2"
)
