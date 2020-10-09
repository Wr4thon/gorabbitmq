package middleware_test

import (
	"errors"
	"log"
	"sync"
	"testing"

	"github.com/Wr4thon/gorabbitmq/v3"
	"github.com/Wr4thon/gorabbitmq/v3/middleware"
)

func Test_Middleware(t *testing.T) {
	connection, err := gorabbitmq.NewConnection(gorabbitmq.ConnectionSettings{
		Host:     "localhost",
		Password: "guest",
		UserName: "guest",
		Port:     5672,
	}, gorabbitmq.ChannelSettings{
		UsePrefetch: false,
	})
	if err != nil {
		t.Log("error while creating the connection", err)
		t.FailNow()
	}

	queue, err := connection.ConnectToQueue(
		gorabbitmq.QueueSettings{
			QueueName:        "test",
			DeleteWhenUnused: false,
			Durable:          true,
			Exclusive:        true,
			NoWait:           false,
		},
		gorabbitmq.WithErrorHandler(func(c gorabbitmq.Context, e error) error {
			return nil
		}),
	)
	if err != nil {
		t.Log("error while connecting to queue", err)
		t.FailNow()
	}

	defer queue.Close()
	queue.SendPlainString("")
	var wg sync.WaitGroup
	wg.Add(6)

	queue.RegisterConsumerAsync(
		gorabbitmq.ConsumerSettings{
			AutoAck:   false,
			Exclusive: true,
			NoLocal:   false,
			NoWait:    false,
		}, func(ctx gorabbitmq.Context) error {
			wg.Done()
			return errors.New("something")
		},
		middleware.ErrorCounterWithConfig(
			middleware.ErrorCounterConfig{
				MaxRetries: 5,
				MaxRetriesExceeded: func(gorabbitmq.Context) {
					log.Print("Exceeded!")
				},
			},
		),
	)

	wg.Wait()
}
