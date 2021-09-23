package gkafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strings"
)

func InitProducer(cfg *Config, f ...FuncCfg) *Engine {
	var err error
	var e = &Engine{}

	if err = e.setSaramaConfig(cfg, f...); err != nil {
		panic(fmt.Sprintf("%s%s", "InitProducer setSaramaConfig err: ", err.Error()))
	}

	producer, err := sarama.NewSyncProducer(strings.Split(cfg.Brokers, ","), e.config)
	log.Printf("[gkafka_producer] InitProducer success.")

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

func (e *Engine) SendMsgs(msgs []*sarama.ProducerMessage) (err error) {
	err = e.producer.SendMessages(msgs)
	return
}
