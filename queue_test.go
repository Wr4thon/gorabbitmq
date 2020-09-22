package gorabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
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

	err = enqueue.Send(context.TODO(), TestTask{ID: 1337})
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
	deliveryConsumer := DeliveryConsumer(func(c context.Context, delivery amqp.Delivery) error {
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

type loggingContextKey struct{}

func TestSendWithContext(t *testing.T) {
	connection, err := NewConnection(ConnectionSettings{
		Host:     "localhost",
		Password: "guest",
		UserName: "guest",
		Port:     5672,
	}, ChannelSettings{
		UsePrefetch: false,
	})

	if err != nil {
		t.Log("error while creating the connection", err)
		t.FailNow()
	}

	inputLoggingContext := map[string]interface{}{
		"foo": "bar",
	}

	queue, err := connection.ConnectToQueue(QueueSettings{
		QueueName:        "test",
		DeleteWhenUnused: true,
		Durable:          false,
		Exclusive:        true,
		NoWait:           false,
	}, WithLoggingContextExtractor(func(c context.Context) (map[string]interface{}, error) {
		return inputLoggingContext, nil
	}), WithLoggingContextBuilder(func(m map[string]interface{}) (context.Context, error) {
		return context.WithValue(context.Background(), loggingContextKey{}, m), nil
	}))

	if err != nil {
		t.Log("error while connecting to queue", err)
		t.FailNow()
	}

	var wg sync.WaitGroup

	go func() {
		queue.ConsumerOnce(ConsumerSettings{
			AutoAck:   false,
			Exclusive: true,
			NoLocal:   false,
			NoWait:    false,
		}, func(ctx context.Context, a amqp.Delivery) error {
			defer wg.Done()

			out, ok := ctx.Value(loggingContextKey{}).(map[string]interface{})
			if !ok {
				t.Fail()
			}

			t.Log(out)

			if !validateContext(inputLoggingContext, out) {
				t.Fail()
			}

			return nil
		})
	}()

	wg.Add(1)
	if err = queue.Send(context.TODO(), TestTask{ID: 1337}); err != nil {
		panic(err)
	}

	wg.Wait()
}

func validateContext(in, out map[string]interface{}) bool {
	return reflect.DeepEqual(in, out)
}
