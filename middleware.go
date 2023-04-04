package gorabbitmq

import (
	"context"

	"github.com/Noobygames/amqp"
)

type (
	// Context is the context, that is passed to the handler and middleware
	// functions.
	Context interface {
		Delivery() amqp.Delivery
		DeliveryContext() context.Context
		Nack(multiple bool, requeue bool) error
		Ack(multiple bool) error
		Body() []byte
		Queue() Queue

		Set(key interface{}, value interface{})
		Value(key interface{}) interface{}
	}

	// MiddlewareFunc defines a function to process middleware.
	MiddlewareFunc func(HandlerFunc) HandlerFunc

	// HandlerFunc defines a function to serve RabbitMQ deliveries.
	HandlerFunc func(Context) error

	gorabbitmqContext struct {
		ctx      context.Context
		delivery amqp.Delivery
		queue    Queue
		values   map[interface{}]interface{}
	}
)

func newContext(ctx context.Context, delivery amqp.Delivery, q Queue) Context {
	return &gorabbitmqContext{
		ctx:      ctx,
		delivery: delivery,
		queue:    q,
		values:   make(map[interface{}]interface{}),
	}
}

func (gc *gorabbitmqContext) Queue() Queue {
	return gc.queue
}

func (gc *gorabbitmqContext) DeliveryContext() context.Context {
	return gc.ctx
}

func (gc *gorabbitmqContext) Delivery() amqp.Delivery {
	return gc.delivery
}

func (gc *gorabbitmqContext) Nack(multiple bool, requeue bool) error {
	return gc.delivery.Nack(multiple, requeue)
}

func (gc *gorabbitmqContext) Ack(multiple bool) error {
	return gc.delivery.Ack(multiple)
}

func (gc *gorabbitmqContext) Body() []byte {
	return gc.delivery.Body
}

func (gc *gorabbitmqContext) Set(key interface{}, value interface{}) {
	gc.values[key] = value
}

func (gc *gorabbitmqContext) Value(key interface{}) interface{} {
	return gc.values[key]
}
