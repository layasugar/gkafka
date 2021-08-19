## go操作kafka

#### 引入包

```
    import "github.com/layasugar/kafka-go"
```

#### producer 生产者
#### protocol(plaintext, sasl_ssl, sasl_plaintext)

```
    var cfg = &KafkaConfig{
        Brokers  string `json:"brokers"`
        Topic    string `json:"topic"`
        Group    string `json:"group"`
        User     string `json:"user"`
        Pwd      string `json:"pwd"`
        Ca       string `json:"ca"`
        Version  string `json:"version"`
        Protocol string `json:"protocol"`
    }
    Producer = gkafka.InitProducer(cfg)
	
    err := Producer.SendMsg(topic, string(dataByte), partition)
```

#### consumer 消费者

```
    consumerData := make(chan *gkafka.ConsumerData)
    go gkafka.InitConsumer(item, consumerData, gkafka.SetClientId("kafka-go"))
    
    for consumerData := range consumerData {
	    logs.Printf("pool submit topic:%q partition:%d offset:%d", data.Topic, data.Partition, data.Offset)
	}
```
