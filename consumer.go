package gkafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

func InitConsumer(cfg *Config, dataChan chan *ConsumerData, f ...FuncCfg) {
	var err error
	var e = &Engine{}

	if err = e.setSaramaConfig(cfg, f...); err != nil {
		panic(fmt.Sprintf("%s%s", "InitConsumer setSaramaConfig err: ", err.Error()))
	}

	consumer := Consumer{
		ready:    make(chan bool),
		dataChan: dataChan,
	}
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(cfg.Brokers, ","), cfg.Group, e.config)
	if nil != err {
		log.Panicf("NewConsumerGroup, err=%s", err.Error())
	}
	//defer client.Close()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err = client.Consume(ctx, []string{cfg.Topic}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready // Await till the consumer has been set up
	log.Printf("[gkafka_consumer] InitConsumer success topic: %v, group: %s.", cfg.Topic, cfg.Group)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	dataChan chan *ConsumerData
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		consumer.dataChan <- &ConsumerData{Msg: message.Value, Topic: message.Topic, Partition: message.Partition, Offset: message.Offset}
		session.MarkMessage(message, "")
	}

	return nil
}
