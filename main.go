package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/corluk/go-utils/kafkaclient"
	"github.com/corluk/go-utils/utils"
	"github.com/joho/godotenv"
)

type User struct {
	Id   int
	Name string
}
type TestMessage struct {
	Value string
}

func main() {
	godotenv.Load()
	brokers := "localhost:9092,localhost:9093,localhost:9094"
	configConsumer := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "group1",
		"auto.offset.reset": "earliest",
	}
	configProducer := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "client2",
	}

	kafkaclient.SetConsumerConfig(configConsumer)
	kafkaclient.SetProducerConfig(configProducer)
	kafkaClient := kafkaclient.GetInstance()

	obj := new(TestMessage)
	value := utils.RandomString(16)
	obj.Value = value
	/*
		kafkaClient.Publish("randomvalue1", obj)
		kafkaClient.Publish("randomvalue1", obj)
		kafkaClient.Publish("randomvalue1", obj)
	*/
	kafkaClient.Subscribe(strings.Split("randomvalue1", ","), func(message []byte) error {
		fmt.Println("received ")
		var testMessage TestMessage
		err := json.Unmarshal(message, &testMessage)
		if err != nil {
			return err
		}

		fmt.Printf("value is %s", testMessage.Value)
		//log.Fatal("incoming message ")
		//obj := incoming.(TestMessage)
		//assert.Equal(t, obj.Value, value)
		//fmt.Println(obj.Value)
		//return errors.New("close connection ")
		return nil
	})

	/* sqllite, err := db.NewSqlLite(os.Getenv("SQLLITE_DB"), &gorm.Config{})
	if err != nil {
		log.Fatalln("cannot create db")
		return
	}
	sqllite.AutoMigrate(&User{})
	var handler server.HandlerMap = server.HandlerMap{
		Method: "GET",
		Url:    "/api/test1",
		Handler: func(c *gin.Context) {
			c.String(http.StatusOK, "ok")
		},
	}
	var restServer server.RestServer = server.RestServer{}
	restServer.Handlers = append(restServer.Handlers, handler)

	server := server.GetInstance()
	server.Rest(restServer)
	kafka, err := kafka.Default()
	if err != nil {
		log.Fatalln("error occu")
		return
	}

	server.Kafka = kafka

	server.Router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")

	})

	server.Router.Run(":2031")
	*/

}
