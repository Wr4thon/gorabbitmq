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
	ConsumerOnce(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error
	GetMessagesCount() int
	/* RegisterConsumer registers a consumer
	reads items from the queue and passes them in the provided callback.
	*/
	RegisterConsumer(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error
	RegisterConsumerAsync(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error
	/* closes the virtual connection (channel) but not the real connection (tcp)
	you need to get e new Queue connection once this method is called
	*/
	Close()
	// Returns wether the channel is closed
	IsClosed() bool
}

type (
	// DeliveryConsumer can be registered as consumer
	DeliveryConsumer func(context.Context, amqp.Delivery) error
)

// ConsumerSettings are uses as settings for amqp
type ConsumerSettings struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

type queue struct {
	Queue
	queueSettings    QueueSettings
	channel          *channel
	queue            amqp.Queue
	loggingExtractor LoggingContextExtractor
	loggingBuilder   LoggingContextBuilder
}

type LoggingContextExtractor func(context.Context) (map[string]interface{}, error)
type LoggingContextBuilder func(map[string]interface{}) (context.Context, error)

type ConfigBuilder func(*queue) error

func WithLoggingContextExtractor(contextExtractor LoggingContextExtractor) ConfigBuilder {
	return func(q *queue) error {
		q.loggingExtractor = contextExtractor

		return nil
	}
}

func WithLoggingContextBuilder(contextBuilder LoggingContextBuilder) ConfigBuilder {
	return func(q *queue) error {
		q.loggingBuilder = contextBuilder

		return nil
	}
}

func (c *queue) SendPlainText(body string) error {
	return c.sendInternal(amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}

func (c *queue) Send(ctx context.Context, body interface{}) error {
	return c.SendWithTable(ctx, body, nil)
}

func (c *queue) SendWithTable(ctx context.Context, body interface{}, table map[string]interface{}) error {
	message, err := json.Marshal(&body)

	if err != nil {
		return err
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

	c.channel.close()
}

func (c *queue) IsClosed() bool {
	return c.channel.closed
}

func (c *queue) Consume(consumerSettings ConsumerSettings) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(c.queueSettings.QueueName, "", consumerSettings.AutoAck, consumerSettings.Exclusive, consumerSettings.NoLocal, consumerSettings.NoWait, nil)
}

func (c *queue) RegisterConsumer(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error {
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

		ctx, err := c.loadContext(item)
		if err != nil {
			return errors.Wrap(err, "error while loading context")
		}

		err = deliveryConsumer(ctx, item)

		if err != nil {
			return errors.Wrap(err, "error while delivering message to consumer")
		}
	}

	return nil
}

func (c *queue) RegisterConsumerAsync(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error {
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
		for item := range channel {
			ctx, err := c.loadContext(item)
			if err != nil {
				// TODO: return errors.Wrap(err, "error while loading context")
			}

			err = deliveryConsumer(ctx, item)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	return nil
}

func (c *queue) ConsumerOnce(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error {
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

	ctx, err := c.loadContext(item)
	if err != nil {
		return errors.Wrap(err, "error while loading context")
	}

	err = deliveryConsumer(ctx, item)
	if err != nil {
		log.Println(err)
	}

	c.Close()

	return nil
}

func (c *queue) GetMessagesCount() int {
	return c.queue.Messages
}

func (c *queue) loadContext(delivery amqp.Delivery) (context.Context, error) {
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

	return ctx, nil
}
