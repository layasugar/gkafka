package gkafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strings"
)

func InitProducer(cfg *Config, f ...FuncCfg) *Engine {
	log.Println("init kafka producer, it may take a few seconds to init the connection.")

	var err error
	var e = &Engine{}

	if err = e.setSaramaConfig(cfg, f...); err != nil {
		panic(fmt.Sprintf("%s%s", "InitProducer setSaramaConfig err: ", err.Error()))
	}

	producer, err := sarama.NewSyncProducer(strings.Split(cfg.Brokers, ","), e.config)

	e.producer = producer
	return e
}

func (e *Engine) SendMsg(topic, message string, partition int32) (err error) {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Value:     sarama.StringEncoder(message),
	}
	_, _, err = e.producer.SendMessage(msg)
	return
}
