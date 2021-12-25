package kafka

import (
	"errors"
	"log"

	"os"
	"os/signal"
	"strings"

	"syscall"

	"github.com/Shopify/sarama"
)

type Listener func([]byte)
type KafkaConnector struct {
	Brokers []string
	Config  *sarama.Config
}

func Default() (*KafkaConnector, error) {

	brokers := strings.Split(os.Getenv("KAFKA_URIS"), ",")
	if len(brokers) < 1 {
		return &KafkaConnector{}, errors.New("no kafka uri defined ")
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Consumer.Return.Errors = true
	return New(brokers, config), nil
}
func New(brokers []string, config *sarama.Config) *KafkaConnector {

	var kafkaConnector KafkaConnector
	kafkaConnector.Brokers = brokers
	kafkaConnector.Config = config

	return &kafkaConnector
}

/*
TODO copy them
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5 // NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.

	if err != nil {
		return nil, err
	}
	return conn, nil
}
*/
func (kafkaConnector *KafkaConnector) Publish(topic string, message []byte) error {
	//brokersUrl := []string{"localhost:9092"}

	brokersUrl := strings.Split(os.Getenv("KAFKA_URIS"), ",")
	if len(brokersUrl) < 1 {
		return errors.New("kafka Uris not defined ")
	}
	producer, err := sarama.NewSyncProducer(kafkaConnector.Brokers, kafkaConnector.Config)

	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		return err
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

/*
func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true // NewConsumer creates a new consumer using the given broker addresses and configuration

	if err != nil {
		return nil, err
	}
	return conn, nil
}
*/
func (kafkaConnector *KafkaConnector) Consume(topic string, listener func([]byte), position int64) error {

	//worker, err := connectConsumer([]string{"localhost:9092"})
	conn, err := sarama.NewConsumer(kafkaConnector.Brokers, kafkaConnector.Config)

	if err != nil {
		return err
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.

	// consumer, err := conn.ConsumePartition(topic, 0, sarama.OffsetNewest)
	consumer, err := conn.ConsumePartition(topic, 0, position)

	if err != nil {
		return err
	}

	log.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				listener(msg.Value)
				log.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	if err := conn.Close(); err != nil {
		return err
	}
	return nil
}
