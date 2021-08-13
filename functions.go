package gkafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
)

func (e *Engine) setSaramaConfig(cfg *KafkaConf, f ...FuncCfg) error {
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

	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

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

func SetClientId(value string) FuncCfg {
	return func(e *Engine) error {
		e.config.ClientID = value
		return nil
	}
}
