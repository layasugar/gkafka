package gkafka

import (
	"github.com/Shopify/sarama"
)

type Engine struct {
	config   *sarama.Config
	dataChan chan *ConsumerData
	producer sarama.SyncProducer
}

type ConsumerData struct {
	Msg       []byte
	Topic     string
	Partition int32
	Offset    int64
}

type KafkaConf struct {
	Brokers  string `json:"brokers"`
	Topic    string `json:"topic"`
	Group    string `json:"group"`
	User     string `json:"user"`
	Pwd      string `json:"pwd"`
	Ca       string `json:"ca"`
	Version  string `json:"version"`
	Protocol string `json:"protocol"`
}

type FuncCfg func(e *Engine) error
