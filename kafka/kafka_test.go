package kafka

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/corluk/go-utils/utils"
	"github.com/stretchr/testify/assert"
)

func Test_should_kafka_receive_sended_message(t *testing.T) {

	brokers := "localhost:9092,localhost:9093,localhost:9094"

	SetBrokers(strings.Split(brokers, ","))
	client, err := Default()
	if err != nil {
		assert.Fail(t, "Cannot connect to default kafka")

	}
	var topic string = utils.RandomString(16)
	var message string = utils.RandomString(32)
	err = client.Publish("-test-"+topic, []byte(message))
	if err != nil {
		assert.Fail(t, "cannot publish topic")
	}
	fmt.Println("receiving")
	client.Subscribe(topic, func(b []byte) {

		incomingmessage := string(b)
		assert.Equal(t, incomingmessage, message)

	}, sarama.OffsetNewest)

}
