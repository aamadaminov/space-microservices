package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Clickhouse start
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "spaceshipomicron_db",
			Username: "username",
			Password: "password",
		},
		// TLS: &tls.Config{},
	})
	if err != nil {
		panic(err)
	}

	// clickhouse version show begin
	v, err := conn.ServerVersion()
	fmt.Println(v)
	if err != nil {
		panic(err)
	}
	// Clickhouse version show end

	// clickhouse check db exist begin
	err2 := conn.Exec(context.Background(), `
    CREATE TABLE IF NOT EXISTS coords1
(
    s1 String,
    s2 String,
) ENGINE = MergeTree()
 PRIMARY KEY s1
`)
	if err2 != nil {
		panic(err2)
	}
	// clickhouse check db exist end

	// kafka start
	config := &kafka.ConfigMap{
		"bootstrap.servers": ":9092,:9093,:9094",
		"group.id":          "fGroup",
		"auto.offset.reset": "smallest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %v", err))
	}
	err = consumer.SubscribeTopics([]string{"sync-topic-coords"}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer initialized")

	run := true
	for run == true {
		ev := consumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			//fmt.Println(string(e.Value))
			str1 := fmt.Sprint(string(e.Value))
			fmt.Println(str1)
			// insert to clickhouse
			//str2 := "abc"
			err3 := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO spaceshipomicron_db.coords1 (s1) VALUES ('%s')", str1))
			if err3 != nil {
				panic(err3)
			}
			//			insert to clickhouse end
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
			//	default:
			//	fmt.Printf("Ignored %v\n", e)

		}

	} // env, err := GetNativeTestEnvironment()
	// if err != nil {
	// 	return err
	// }
	consumer.Close()
}
