package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "message-log"
	brokerAddress = "localhost:9092"
)

func main() {
	context := context.Background()

	createTopic(context)
	go produce(context)
	consume(context)
}

func createTopic(context context.Context) {
	conn, err := kafka.DialLeader(context, "tcp", "localhost:9092", topic, 0)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
}

func produce(ctx context.Context) {

	i := 0
	logger := log.New(os.Stdout, "kafka producer: ", 0)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		Logger:  logger,
	})

	for {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("this is message" + strconv.Itoa(i)),
		})

		if err != nil {
			panic("could not write message " + err.Error())
		}

		fmt.Println("writes:", i)
		i++

		time.Sleep(time.Second)
	}
}

func consume(ctx context.Context) {
	logger := log.New(os.Stdout, "kafka consumer: ", 0)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "my-group",
		Logger:  logger,
	})

	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))
	}
}
