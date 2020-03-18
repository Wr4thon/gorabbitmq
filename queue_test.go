package gorabbitmq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Noobygames/amqp"
	"github.com/labstack/gommon/log"
	"github.com/pkg/errors"
)

// TestTask is used to test
type TestTask struct {
	ID int `json:"id,omitempty"`
}

func TestConsume(t *testing.T) {
	enqueue, err := connectToRabbit("test_queue")
	if err != nil {
		t.Error(err)
	}

	err = enqueue.Send(TestTask{ID: 1337})
	if err != nil {
		t.Error("Could not enqueue Task: ", err)
	}

	fetchqueue, err := connectToRabbit("test_queue")
	if err != nil {
		t.Error(err)
	}

	go consume(fetchqueue, t)
	time.Sleep(500 * time.Millisecond)
}

func consume(queue Queue, t *testing.T) {
	consumerSettings := ConsumerSettings{AutoAck: false, Exclusive: false, NoLocal: false, NoWait: false}
	deliveryConsumer := DeliveryConsumer(func(delivery amqp.Delivery) error {
		if queue.IsClosed() {
			return errors.New("queue channel was closed")
		}

		var task TestTask

		err := json.Unmarshal(delivery.Body, &task)
		if err != nil {
			log.Error(err)

			nackError := delivery.Nack(false, true)
			if nackError != nil {
				fmt.Println("Failed to nack message")
			}

			return err
		}

		time.Sleep(10 * time.Millisecond)

		fmt.Println("Successfully handled message")

		err = delivery.Ack(false)
		if err != nil {
			fmt.Println("Failed to ack message")
		}

		return nil
	})

	err := queue.RegisterConsumer(consumerSettings, deliveryConsumer)
	if err != nil {
		log.Error(err)
	}
}

func connectToRabbit(queueName string) (Queue, error) {
	rabbitMQHost := "localhost"
	if rabbitMQHost == "" {
		const errMessage = "RabbitMQHost must not be empty"
		return nil, errors.New(errMessage)
	}

	rabbitMQUser := "guest"
	if rabbitMQUser == "" {
		const errMessage = "RabbitMQUser must not be empty"
		return nil, errors.New(errMessage)
	}

	rabbitMQPassword := "guest"
	if rabbitMQPassword == "" {
		const errMessage = "RabbitMQPassword must not be empty"
		return nil, errors.New(errMessage)
	}

	const rabbitPort = 5672

	connectionSettings := ConnectionSettings{
		UserName: rabbitMQUser,
		Password: rabbitMQPassword,
		Host:     rabbitMQHost,
		Port:     rabbitPort,
	}

	channelSettings := ChannelSettings{}

	rabbit, err := NewConnection(connectionSettings, channelSettings)
	if err != nil {
		const errMessage = "Failed to initialize rabbitmq"
		return nil, errors.Wrap(err, errMessage)
	}

	settings := QueueSettings{
		QueueName:        queueName,
		Durable:          true,
		DeleteWhenUnused: false,
		Exclusive:        false,
		NoWait:           false,
	}

	rabbitQueue, err := rabbit.ConnectToQueue(settings)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to rabbitMQ")
	}

	return rabbitQueue, nil
}
