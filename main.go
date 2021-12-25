package main

import (
	"log"
	"net/http"

	"github.com/corluk/go-utils/kafka"
	"github.com/corluk/go-utils/server"
	"github.com/gin-gonic/gin"
)

func main() {
	go
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

}
