package gkafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
)

func (e *Engine) setSaramaConfig(cfg *Config, f ...FuncCfg) error {
	var err error
	config := sarama.NewConfig()

	if cfg.Version != "" {
		config.Version, err = sarama.ParseKafkaVersion(cfg.Version)
		if nil != err {
			return errors.New(fmt.Sprintf("ParseKafkaVersion, err=%s", err.Error()))
		}
	}

	switch cfg.Protocol {
	case "plaintext":
	case "sasl_ssl":
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.User
		config.Net.SASL.Password = cfg.Pwd
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

		config.Net.TLS.Enable = true
		var tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		certBytes, err := ioutil.ReadFile(cfg.Ca)
		if nil != err {
			return errors.New(fmt.Sprintf("Ca is fail, err=%s", err.Error()))
		}
		clientCertPool := x509.NewCertPool()
		ok := clientCertPool.AppendCertsFromPEM(certBytes)
		if !ok {
			return errors.New("AppendCertsFromPEM fail")
		}
		tlsConfig.RootCAs = clientCertPool
		config.Net.TLS.Config = tlsConfig
	case "sasl_plaintext":
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.User
		config.Net.SASL.Password = cfg.Pwd
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		//if cfg.Scram == "sha512" {
		//	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		//	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		//}
		//
		//if cfg.Scram == "sha256" {
		//	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		//	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		//}
	}

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewManualPartitioner // 随机分区
	//config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin // 默认是range

	e.config = config
	for _, fe := range f {
		if err = fe(e); err != nil {
			return errors.New(fmt.Sprintf("setSaramaConfig config, err=%s", err.Error()))
		}
	}

	if err = config.Validate(); nil != err {
		return errors.New(fmt.Sprintf("setSaramaConfig config.Validate(), err=%s", err.Error()))
	}
	return nil
}

func SetClientId(v string) FuncCfg {
	return func(e *Engine) error {
		e.config.ClientID = v
		return nil
	}
}

func SetOffsetsInitial(v int64) FuncCfg {
	return func(e *Engine) error {
		e.config.Consumer.Offsets.Initial = v
		return nil
	}
}

func SetPartitioner(v sarama.PartitionerConstructor) FuncCfg {
	return func(e *Engine) error {
		e.config.Producer.Partitioner = v
		return nil
	}
}

func SetReBalanceStrategy(v sarama.BalanceStrategy) FuncCfg {
	return func(e *Engine) error {
		e.config.Consumer.Group.Rebalance.Strategy = v
		return nil
	}
}

func SetRequiredAcks(v sarama.RequiredAcks) FuncCfg {
	return func(e *Engine) error {
		e.config.Producer.RequiredAcks = v
		return nil
	}
}
