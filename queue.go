package gorabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

// Queue is the main interaction interface with the RabbitMQ Server
type Queue interface {
	SendPlainString(body string) error
	Send(body interface{}) error
	Consume(autoAck, exclusive, noLocal, noWait bool) (<-chan amqp.Delivery, error)
	Close()
}

type queue struct {
	Queue
	queueSettings QueueSettings
	channel       *amqp.Channel
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
	return c.channel.Publish(
		"",
		c.queueSettings.QueueName,
		false,
		false,
		publishing,
	)
}

func (c *queue) Close() {
	c.channel.Close()
}

func (c *queue) Consume(autoAck, exclusive, noLocal, noWait bool) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(c.queueSettings.QueueName, "", autoAck, exclusive, noLocal, noWait, nil)
}
