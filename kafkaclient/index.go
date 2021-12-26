package kafkaclient

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Handler func(message []byte) error
type KafkaConnector struct {
	ProducerConfig *kafka.ConfigMap
	ConsumerConfig *kafka.ConfigMap
}

var kafkaConnector *KafkaConnector = new(KafkaConnector)

func SetProducerConfig(kafkaConfig *kafka.ConfigMap) {
	kafkaConnector.ProducerConfig = kafkaConfig

}
func SetConsumerConfig(kafkaConfig *kafka.ConfigMap) {
	kafkaConnector.ConsumerConfig = kafkaConfig

}
func GetInstance() *KafkaConnector {

	return kafkaConnector

}

func New(config *kafka.ConfigMap) KafkaConnector {

	var connector KafkaConnector
	connector.ProducerConfig = config
	return connector
}

func (kafkaConnector *KafkaConnector) Publish(topic string, object interface{}) error {

	p, err := kafka.NewProducer(kafkaConnector.ProducerConfig)
	if err != nil {
		return err
	}
	go func() {
		for e := range p.Events() {
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
	defer p.Close()
	obj, err := json.Marshal(object)
	if err != nil {
		return err
	}
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          obj,
	}
	err = p.Produce(kafkaMessage, nil)
	if err != nil {
		return err
	}
	p.Flush(3 * 1000)
	return nil
}

func (kafkaConnector *KafkaConnector) Subscribe(topic []string, handler Handler) error {

	consumer, err := kafka.NewConsumer(kafkaConnector.ConsumerConfig)

	consumer.SubscribeTopics(topic, nil)
	if err != nil {
		return err
	}

	//run := true
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			handler(msg.Value)

			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
		/*
			fmt.Println("listeninng")
			message, err := consumer.ReadMessage(10)
			fmt.Println("received message")
			if err != nil {
				log.Fatalf("error occured %s", err)

				//run = false
			}

			value := new(interface{})
			json.Unmarshal(message.Value, &value)
			fmt.Println("unserialized")

			fmt.Println(string(message.Value))
			err = handler(value, consumer)
			fmt.Println("error")
			fmt.Println(err != nil)
			if err != nil {
				log.Fatalf("error occured %s", err)
				//run = false
			}
			/*
				event := consumer.Poll(0)

				switch e := event.(type) {
				case *kafka.Message:

					value := new(interface{})
					json.Unmarshal(e.Value, &value)
					err = handler(value, consumer)
					if err != nil {
						run = false
					}
				case *kafka.PartitionEOF:
					log.Fatalln("reached end")
				case *kafka.Error:
					run = false
				default:
					fmt.Println(e)
				}
		*/
	}
	//defer consumer.Close()
	return nil
}
