package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("listening messages")
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-go-starter_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest", // le todas as mensagens
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		fmt.Println("error consumer", err.Error())
	}

	topics := []string{"teste"}
	c.SubscribeTopics(topics, nil)
	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}
