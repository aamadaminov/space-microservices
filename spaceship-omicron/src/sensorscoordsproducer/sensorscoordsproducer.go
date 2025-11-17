package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pb "sensorscoordsproducer/sensorproto"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// kafka start
	// Настройка конфигурации продюсера
	// config := &kafka.ConfigMap{
	// 	"bootstrap.servers": "192.168.18.138:9092,192.168.18.138:9093,192.168.18.138:9094",
	// 	"acks":              "all",                                                         // Ожидание подтверждения от всех реплик
	// 	"client.id":         "myProducer",
	// }
	config := &kafka.ConfigMap{
		"bootstrap.servers": ":9092,:9093,:9094",
		"acks":              "all",
		"client.id":         "myProducer",
	}

	// Инициализация продюсера
	producer, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	defer producer.Close()
	fmt.Println("Producer initialized")
	// kafka end

	conn, err := grpc.NewClient("localhost:50070", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Error starting client: %v", err)
	}
	defer conn.Close()

	client := pb.NewSensorServiceClient(conn)
	for {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.GetSensor(ctx, &pb.SensorRequest{})
		if err != nil {
			log.Fatalf("Error call GetSensor: %v", err)
		}
		coordsString1 := fmt.Sprintf("Time %s", res.T)
		coordsString2 := fmt.Sprintf("Received coords: X=%f, Y=%f, Z=%f", res.X, res.Y, res.Z)

		// kafka begin
		// Создание канала доставки
		deliveryChan := make(chan kafka.Event)

		// Отправка сообщений в синхронном режиме
		topic := "sync-topic-coords"
		for _, word := range []string{coordsString1, coordsString2} {
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(word),
			}, deliveryChan)

			// Ожидание события доставки
			event := <-deliveryChan
			m := event.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}

		// Закрытие канала подтверждений
		close(deliveryChan)
		// kafka end

		time.Sleep(5 * time.Second)
	}

}
