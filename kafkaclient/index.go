package kafkaclient

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Handler func(message []byte) error
type KafkaConnector struct {
}

var kafkaConnector *KafkaConnector = new(KafkaConnector)

func GetInstance() *KafkaConnector {

	return kafkaConnector

}

func New() KafkaConnector {

	var connector KafkaConnector

	return connector
}

func (kafkaConnector *KafkaConnector) Publish(kafkaProducer *kafka.Producer, topic string, object interface{}) error {

	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	defer kafkaProducer.Close()
	obj, err := json.Marshal(object)
	if err != nil {
		return err
	}
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          obj,
	}
	err = kafkaProducer.Produce(kafkaMessage, nil)
	if err != nil {
		return err
	}
	kafkaProducer.Flush(3 * 1000)
	return nil
}

func (kafkaConnector *KafkaConnector) Subscribe(consumer *kafka.Consumer, topic []string, handler Handler) error {

	consumer.SubscribeTopics(topic, nil)
	fmt.Println(consumer)
	//run := true
	run := true
	for run {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {

			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			run = false
		}
		fmt.Println(msg)
		err = handler(msg.Value)
		if err != nil {
			run = false
		}

	}

	defer consumer.Close()
	return nil
}
