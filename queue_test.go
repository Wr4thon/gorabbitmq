package gorabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/labstack/gommon/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
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
	deliveryConsumer := func(context Context) error {
		if queue.IsClosed() {
			return errors.New("queue channel was closed")
		}

		var task TestTask
		err := json.Unmarshal(context.Delivery().Body, &task)
		if err != nil {
			log.Error(err)

			nackError := context.Nack(false, true)
			if nackError != nil {
				fmt.Println("Failed to nack message")
			}

			return err
		}

		time.Sleep(10 * time.Millisecond)
		fmt.Println("Successfully handled message")

		err = context.Ack(false)
		if err != nil {
			fmt.Println("Failed to ack message")
		}

		return nil
	}

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

	rabbit, err := NewConnection(connectionSettings, ChannelSettings{})
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
	}, WithContextExtractor(func(c context.Context) (map[string]interface{}, error) {
		return inputLoggingContext, nil
	}), WithContextBuilder(func(m map[string]interface{}) (context.Context, error) {
		return context.WithValue(context.Background(), loggingContextKey{}, m), nil
	}))

	if err != nil {
		t.Log("error while connecting to queue", err)
		t.FailNow()
	}

	var wg sync.WaitGroup

	go func() {
		queue.ConsumeOnce(ConsumerSettings{
			AutoAck:   false,
			Exclusive: true,
			NoLocal:   false,
			NoWait:    false,
		}, func(ctx Context) error {
			defer wg.Done()

			out, ok := ctx.DeliveryContext().Value(loggingContextKey{}).(map[string]interface{})
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

func Test_Middleware(t *testing.T) {
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

	queue, err := connection.ConnectToQueue(QueueSettings{
		QueueName:        "test",
		DeleteWhenUnused: true,
		Durable:          false,
		Exclusive:        true,
		NoWait:           false,
	})
	if err != nil {
		t.Log("error while connecting to queue", err)
		t.FailNow()
	}

	queue.SendPlainString("")
	mw := Middleware()
	queue.ConsumeOnce(
		ConsumerSettings{
			AutoAck:   false,
			Exclusive: true,
			NoLocal:   false,
			NoWait:    false,
		}, func(ctx Context) error {
			return errors.New("something")
		}, mw)
}

type customContext struct {
	Context
	errorCounter int
}

func Middleware() MiddlewareFunc {
	return func(hf HandlerFunc) HandlerFunc {
		return func(c Context) error {
			ctx := customContext{
				Context: c,
			}

			if v, ok := ctx.Delivery().Headers["@errorCounter"]; ok {
				if i, ok := v.(int); ok {
					ctx.errorCounter = i
				}
			}

			err := hf(ctx)
			if err != nil {
				ctx.errorCounter++
				ctx.Ack(false)
				table := ctx.Delivery().Headers
				if table == nil {
					table = make(map[string]interface{})
				}

				table["@errorCounter"] = ctx.errorCounter
				c.Queue().SendWithTable(ctx.Context.DeliveryContext(), ctx.Delivery().Body, table)
			}

			return err
		}
	}
}

func Test_CorrelationID(t *testing.T) {
	const correlationID string = "0123456789"

	connSettings := ConnectionSettings{
		Host:     "localhost",
		Port:     5672,
		UserName: "guest",
		Password: "guest",
	}
	chanSettings := ChannelSettings{}

	connector, err := NewConnection(connSettings, chanSettings)
	require.NoError(t, err)

	qSettings := QueueSettings{
		QueueName:        "test_queue",
		Durable:          true,
		Exclusive:        false,
		DeleteWhenUnused: false,
		NoWait:           false,
	}

	queue, err := connector.ConnectToQueue(qSettings)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), keyCorrelationID, correlationID), 5*time.Second)
	defer cancel()

	t.Logf("publishing message with correlationID: %s\n", ctx.Value(keyCorrelationID))

	err = queue.Send(ctx, "Hello World!")
	require.NoError(t, err)

	go consumeCorrelationID(queue, t)

	time.Sleep(500 * time.Millisecond)
}

func consumeCorrelationID(queue Queue, t *testing.T) {
	var err error
	consumerSettings := ConsumerSettings{
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
	}

	handlerFunc := func(ctx Context) error {
		require.False(t, queue.IsClosed())

		correlationID, ok := ctx.DeliveryContext().Value(keyCorrelationID).(string)
		if !ok {
			err = ctx.Nack(false, true)
			require.NoError(t, err)

			return errors.New("no correlation id")
		}

		t.Logf("received message with correlationID: %s\n", correlationID)

		if err = ctx.Ack(false); err != nil {
			require.NoError(t, err)
		}

		return nil
	}

	err = queue.RegisterConsumer(consumerSettings, handlerFunc)
	require.NoError(t, err)
}
