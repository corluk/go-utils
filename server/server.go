package server

import (
	"context"
	"strings"

	"github.com/corluk/go-utils/kafkaclient"
	"github.com/corluk/go-utils/redisclient"
	"github.com/gin-gonic/gin"
)

type Server struct {
	Router *gin.Engine
	Redis  *redisclient.RedisClient
	Kafka  *kafkaclient.KafkaConnector
}
type HandlerMap struct {
	Method  string
	Url     string
	Handler gin.HandlerFunc
}
type RestServer struct {
	Handlers []HandlerMap
}

func (server *Server) Rest(restServer RestServer) {

	for _, handlerMap := range restServer.Handlers {

		method := strings.ToUpper(handlerMap.Method)
		switch method {
		case "GET":
			server.Router.GET(handlerMap.Url, handlerMap.Handler)

		case "POST":
			server.Router.POST(handlerMap.Url, handlerMap.Handler)
		case "HEAD":
			server.Router.HEAD(handlerMap.Url, handlerMap.Handler)
		case "OPTIONS":
			server.Router.OPTIONS(handlerMap.Url, handlerMap.Handler)
		case "PUT":
			server.Router.PUT(handlerMap.Url, handlerMap.Handler)
		case "DELETE":
			server.Router.DELETE(handlerMap.Url, handlerMap.Handler)
		case "PATCH":
			server.Router.PATCH(handlerMap.Url, handlerMap.Handler)

		}

	}
}

func (server *Server) SetRedis(redisUri string, context context.Context) error {

	redis, err := redisclient.New(redisUri, context)

	if err != nil {
		return err
	}

	server.Redis = redis
	return nil
}

func (server *Server) SetKafka(kafkaClient *kafkaclient.KafkaConnector) {

	server.Kafka = kafkaClient

}

var server *Server = nil

func GetInstance() *Server {

	if server == nil {
		server = New()

	}

	return server
}

func New() *Server {

	server := new(Server)
	server.Router = gin.Default()

	return server
}
