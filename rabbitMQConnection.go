package gorabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type RabbitMQConnection interface {
	Close() error
	SendPlainString(body string) error
	Send(body interface{}) error
	Consume(autoAck, exclusive, noLocal, noWait bool) (<-chan amqp.Delivery, error)
}

type rabbitMQConnection struct {
	RabbitMQConnection
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    amqp.Queue
	settings QueueSettings
}

func (c *rabbitMQConnection) SendPlainString(body string) error {

	return c.sendInternal(amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}
func (c *rabbitMQConnection) Send(body interface{}) error {
	message, err := json.Marshal(&body)

	if err != nil {
		return err
	}

	return c.sendInternal(amqp.Publishing{
		ContentType: "application/json",
		Body:        message,
	})
}

func (c *rabbitMQConnection) sendInternal(publishing amqp.Publishing) error {
	return c.channel.Publish(
		"",
		c.settings.QueueName,
		false,
		false,
		publishing,
	)
}

func (c *rabbitMQConnection) Close() error {
	c.channel.Close()
	return c.conn.Close()
}

func (c *rabbitMQConnection) Consume(autoAck, exclusive, noLocal, noWait bool) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(c.settings.QueueName, "", autoAck, exclusive, noLocal, noWait, nil)
}
