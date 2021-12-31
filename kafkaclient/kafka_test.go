package kafkaclient

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TestMessage struct {
	Value string
}

func Test_send_message(t *testing.T) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	client := GetInstance()
	client.Publish(p, "topic-X", "some text")

}

func Test_recieve_message(t *testing.T) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	kafkaClient := GetInstance()
	kafkaClient.Subscribe(c, []string{"topic-X"}, func(value []byte) error {

		fmt.Println(string(value))
		return nil

	})
}
