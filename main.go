package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"sync"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	connRabbit := os.Args[1]
	if connRabbit == "" {
		panic("RABBITMQ_HOST environment variable not set")
	}

	log.Println("connRabbit: ", connRabbit)

	// amqp.Dial("amqp://guest:guest@localhost:5672/")
	conn, err := amqp.Dial(connRabbit)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// init wg
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		func() {
			for i := 0; i < 1000; i++ {
				body := "Hello World!"
				err = ch.PublishWithContext(ctx,
					"",     // exchange
					q.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					})
				failOnError(err, "Failed to publish a message")
				log.Printf(" [x] Sent %s\n", body)
			}

			wg.Done()
		}()

	}

	wg.Wait()

}
