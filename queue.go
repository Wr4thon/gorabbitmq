package gorabbitmq

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Noobygames/amqp"
	"github.com/pkg/errors"
)

const keyDeliveryContext string = "@context"

// Queue is the main interaction interface with the RabbitMQ Server
type Queue interface {
	SendPlainString(body string) error
	Send(ctx context.Context, body interface{}) error
	SendWithTable(ctx context.Context, body interface{}, table map[string]interface{}) error
	Consume(consumerSettings ConsumerSettings) (<-chan amqp.Delivery, error)
	ConsumeOnce(consumerSettings ConsumerSettings, deliveryConsumer HandlerFunc, middleware ...MiddlewareFunc) error
	GetMessagesCount() int
	/* RegisterConsumer registers a consumer
	reads items from the queue and passes them in the provided callback.
	*/
	RegisterConsumer(consumerSettings ConsumerSettings, deliveryConsumer HandlerFunc, middleware ...MiddlewareFunc) error
	RegisterConsumerAsync(consumerSettings ConsumerSettings, deliveryConsumer HandlerFunc, middleware ...MiddlewareFunc) error
	/* closes the virtual connection (channel) but not the real connection (tcp)
	you need to get e new Queue connection once this method is called
	*/
	Close()
	// Returns wether the channel is closed
	IsClosed() bool
}

// ConsumerSettings are uses as settings for amqp
type ConsumerSettings struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

type queue struct {
	queueSettings    QueueSettings
	channel          *channel
	queue            amqp.Queue
	loggingExtractor ContextExtractor
	loggingBuilder   ContextBuilder

	errorChan chan<- error
	doneChan  chan struct{}
	async     bool

	errorHandler ErrorHandler
}

type ContextExtractor func(context.Context) (map[string]interface{}, error)
type ContextBuilder func(map[string]interface{}) (context.Context, error)

// ConfigBuilder is the function type that is called, when
type ConfigBuilder func(*queue) error

// ErrorHandler is the function type that can get called, when an error occurres
// when processing a delivery
type ErrorHandler func(Context, error) error

// DeliveryHandler is the function type that can get called, before or after
// a delivery is received.
type DeliveryHandler func(amqp.Delivery) error

func WithErrorHandler(errorHandler ErrorHandler) ConfigBuilder {
	return func(q *queue) error {
		q.errorHandler = errorHandler
		return nil
	}
}

func WithLoggingContextExtractor(contextExtractor ContextExtractor) ConfigBuilder {
	return func(q *queue) error {
		q.loggingExtractor = contextExtractor

		return nil
	}
}

func WithLoggingContextBuilder(contextBuilder ContextBuilder) ConfigBuilder {
	return func(q *queue) error {
		q.loggingBuilder = contextBuilder

		return nil
	}
}

func (c *queue) SendPlainString(body string) error {
	return c.sendInternal(amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}

func (c *queue) Send(ctx context.Context, body interface{}) error {
	return c.SendWithTable(ctx, body, nil)
}

func (c *queue) SendWithTable(ctx context.Context, body interface{}, table map[string]interface{}) error {
	var message []byte
	switch t := body.(type) {
	case []byte:
		message = t
	case string:
		message = []byte(t)
	default:
		var err error
		message, err = json.Marshal(&body)
		if err != nil {
			return err
		}
	}

	if c.loggingExtractor != nil {
		loggingCtx, err := c.loggingExtractor(ctx)
		if err != nil {
			return errors.Wrap(err, "error while trying to extract loggingContext from passed context")
		}

		if table == nil {
			table = make(map[string]interface{})
		}

		table[keyDeliveryContext] = amqp.Table(loggingCtx)
	}

	return c.sendInternal(amqp.Publishing{
		ContentType: "application/json",
		Body:        message,
		Headers:     table,
	})
}

func (c *queue) sendInternal(publishing amqp.Publishing) error {
	return c.channel.Publish("", c.queueSettings.QueueName, false, false, publishing)
}

func (c *queue) Close() {
	if c.channel.closed {
		return
	}

	if c.async {
		c.doneChan <- struct{}{}
	}

	c.channel.close()
}

func (c *queue) IsClosed() bool {
	return c.channel.closed
}

func (c *queue) Consume(consumerSettings ConsumerSettings) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(c.queueSettings.QueueName, "", consumerSettings.AutoAck, consumerSettings.Exclusive, consumerSettings.NoLocal, consumerSettings.NoWait, nil)
}

type queueError struct {
	cause      error
	innerError error
	message    string
}

func (q queueError) Error() string {
	return q.message
}

func (c *queue) RegisterConsumer(consumerSettings ConsumerSettings, deliveryConsumer HandlerFunc, middleware ...MiddlewareFunc) error {
	c.async = false
	channel, err := c.channel.Consume(
		c.queueSettings.QueueName,
		"",
		consumerSettings.AutoAck,
		consumerSettings.Exclusive,
		consumerSettings.NoLocal,
		consumerSettings.NoWait,
		nil,
	)

	if err != nil {
		return err
	}

	for item := range channel {
		err := c.consumeItem(item, deliveryConsumer, middleware)
		if err != nil && c.errorChan != nil {
			c.errorChan <- err
		}
	}

	return nil
}

func (c *queue) RegisterConsumerAsync(consumerSettings ConsumerSettings, deliveryConsumer HandlerFunc, middleware ...MiddlewareFunc) error {
	c.async = true
	channel, err := c.channel.Consume(
		c.queueSettings.QueueName,
		"",
		consumerSettings.AutoAck,
		consumerSettings.Exclusive,
		consumerSettings.NoLocal,
		consumerSettings.NoWait,
		nil,
	)

	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case item := <-channel:
				err = c.consumeItem(item, deliveryConsumer, middleware)
				if err != nil {
					log.Println(err)
				}
			case <-c.doneChan:
				break
			}
		}
	}()

	return nil
}

func (c *queue) ConsumeOnce(consumerSettings ConsumerSettings, deliveryConsumer HandlerFunc, middleware ...MiddlewareFunc) error {
	defer c.Close()
	channel, err := c.channel.Consume(
		c.queueSettings.QueueName,
		"",
		consumerSettings.AutoAck,
		consumerSettings.Exclusive,
		consumerSettings.NoLocal,
		consumerSettings.NoWait,
		nil,
	)
	if err != nil {
		return err
	}

	item := <-channel

	return c.consumeItem(item, deliveryConsumer, middleware)
}

func (c *queue) consumeItem(item amqp.Delivery, deliveryConsumer HandlerFunc, middleware []MiddlewareFunc) error {
	queueContext, err := c.loadContext(item)
	if err != nil {
		return errors.Wrap(err, "error while loading context")
	}

	h := applyMiddleware(deliveryConsumer, middleware...)

	err = h(queueContext)

	if err != nil {
		err := queueError{
			innerError: err,
			cause:      err,
			message:    "error while executing deliveryConsumer",
		}

		if c.errorHandler == nil {
			return errors.Wrap(err, "error while delivering message to consumer")
		}

		if handlerErr := c.errorHandler(queueContext, err); handlerErr != nil {
			err.innerError = handlerErr
			return err
		}
	}

	return nil
}

func (c *queue) GetMessagesCount() int {
	return c.queue.Messages
}

func (c *queue) loadContext(delivery amqp.Delivery) (Context, error) {
	ctx := context.Background()
	var err error

	if c.loggingBuilder != nil {

		contextMap := make(map[string]interface{})
		if table, ok := delivery.Headers[keyDeliveryContext].(amqp.Table); ok {
			contextMap = map[string]interface{}(table)
		}

		ctx, err = c.loggingBuilder(contextMap)
		if err != nil {
			return nil, errors.Wrap(err, "error wile loading Context from passed value")
		}
	}

	return newContext(ctx, delivery, c), nil

}

func applyMiddleware(h HandlerFunc, middleware ...MiddlewareFunc) HandlerFunc {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}
