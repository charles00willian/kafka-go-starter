package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChannel := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("Mensagem Com key", "teste", producer, []byte("transferencia"), deliveryChannel)
	DeliveryReport(deliveryChannel) //esta em outra thread assincrona

	// Pega o resultado do envio da mensagem
	// PS: nao esta legal pois ficou sincrono
	// e := <-deliveryChannel
	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar")
	// } else {
	// 	fmt.Println("Mensagem enviada: ", msg.TopicPartition)
	// }

	// producer.Flush(10000) // espera o retorno do producer se nao o go nao envia corretamente a mensagem, matando o processo
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka-go-starter_kafka_1:9092",
		"delivery.timeout.ms": "0", // tempo maximo de espera de uma mensagem
		"acks":                "all",
		"enable.idempotence":  "true", // se true, acks DEVE ser all
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChannel chan kafka.Event) error {
	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
				// anotar do banco de dados que a mensagem foi processada
			}

		}
	}
	return nil
}
