package kafkaclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/corluk/go-utils/utils"
)

type TestMessage struct {
	Value string
}

func Test_should_kafka_receive_sended_message(t *testing.T) {

	brokers := "localhost:9092,localhost:9093,localhost:9094"
	configConsumer := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "group1" + utils.RandomString(5),
		"auto.offset.reset": "earliest",
	}
	configProducer := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "client2" + utils.RandomString(5),
	}
	kafkaClient := GetInstance()
	SetConsumerConfig(configConsumer)
	SetProducerConfig(configProducer)

	obj := new(TestMessage)
	value := utils.RandomString(16)
	obj.Value = value
	kafkaClient.Subscribe(strings.Split("randomvalue1", ","), func(message []byte) error {
		fmt.Println("received ")
		var testMessage TestMessage
		err := json.Unmarshal(message, &testMessage)
		if err != nil {
			return err
		}

		fmt.Printf("value is %s", testMessage.Value)

		return errors.New("some error to stop")
	})

}
