package gorabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

// Queue is the main interaction interface with the RabbitMQ Server
type Queue interface {
	SendPlainString(body string) error
	Send(body interface{}) error
	Consume(consumerSettings ConsumerSettings) (<-chan amqp.Delivery, error)
	// registeres a consumer
	// reads items from the queue and passes them in the provided callback.
	// WIP this is a blocking call (this will propably change in the future)
	RegisterConsumer(consumerSettings ConsumerSettings, deliveryConsumer DeliveryConsumer) error
	// closes the virtual connection (channel) but not the real connection (tcp)
	// you need to get e new Queue connection once this method is called
	Close()
	// Returns wether the channel is closed
	IsClosed() bool
}

type (
	DeliveryConsumer func(amqp.Delivery) error
)

type ConsumerSettings struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

type queue struct {
	Queue
	queueSettings QueueSettings
	channel       *channel
	queue         amqp.Queue
}

func (c *queue) SendPlainText(body string) error {

	return c.sendInternal(amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}

func (c *queue) Send(body interface{}) error {
	message, err := json.Marshal(&body)

	if err != nil {
		return err
	}

	return c.sendInternal(amqp.Publishing{
		ContentType: "application/json",
		Body:        message,
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
	channel, err := c.channel.Consume(c.queueSettings.QueueName, "", consumerSettings.AutoAck, consumerSettings.Exclusive, consumerSettings.NoLocal, consumerSettings.NoWait, nil)
	if err != nil {
		return err
	}

	for item := range channel {
		deliveryConsumer(item)
	}

	return nil
}
