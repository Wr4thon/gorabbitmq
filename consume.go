package gorabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (

	// Consumer is a consumer for AMQP messages.
	Consumer struct {
		conn    *Connection
		options *ConsumeOptions
		handler HandlerFunc
	}

	// Delivery captures the fields for a previously delivered message resident in
	// a queue to be delivered by the server to a consumer from Channel. Consume or
	// Channel.Get.
	Delivery struct {
		amqp.Delivery
	}

	// HandlerFunc defines the handler of each Delivery and return Action.
	HandlerFunc func(d Delivery) Action
)

// NewConsumer creates a new Consumer instance. Options can be passed to customize the behavior of the Consumer.
func NewConsumer(conn *Connection, queueName string, handler HandlerFunc, options ...ConsumeOption) (*Consumer, error) {
	const errMessage = "failed to create consumer: %w"

	opt := defaultConsumerOptions()

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	opt.QueueOptions.name = queueName

	consumer := &Consumer{
		conn:    conn,
		options: opt,
		handler: handler,
	}

	if err := consumer.startConsuming(); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	consumer.watchRecoverConsumerChan()

	consumer.conn.consumerSubscribed = true

	return consumer, nil
}

func (c *Consumer) watchRecoverConsumerChan() {
	go func() {
		for {
			select {
			case <-c.conn.recoverConsumerChan:
				c.conn.consumerRecoveredChan <- c.startConsuming()
			case <-c.conn.closeChan:
				return
			}
		}
	}()
}

// Close stops consuming messages from the subscribed queue.
func (c *Consumer) Close() error {
	const errMessage = "failed to unsubscribe consumer: %w"

	if err := c.conn.amqpChannel.Cancel(c.options.ConsumerOptions.Name, false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.conn.consumerSubscribed = false

	return nil
}

// RemoveQueue removes the queue from the server including all bindings then purges the messages based on
// server configuration, returning the number of messages purged.
//
// When ifUnused is true, the queue will not be deleted if there are any consumers on the queue.
// If there are consumers, an error will be returned and the channel will be closed.
//
// When ifEmpty is true, the queue will not be deleted if there are any messages remaining on the queue.
// If there are messages, an error will be returned and the channel will be closed.
func (c *Consumer) RemoveQueue(name string, ifUnused bool, ifEmpty bool, noWait bool) (int, error) {
	const errMessage = "failed to remove queue: %w"

	removedMessages, err := c.conn.amqpChannel.QueueDelete(name, ifUnused, ifEmpty, noWait)
	if err != nil {
		return 0, fmt.Errorf(errMessage, err)
	}

	return removedMessages, nil
}

// RemoveBinding removes a binding between an exchange and queue matching the key and arguments.
//
// It is possible to send and empty string for the exchange name which means to unbind the queue from the default exchange.
func (c *Consumer) RemoveBinding(queueName string, routingKey string, exchangeName string, args Table) error {
	const errMessage = "failed to remove binding: %w"

	err := c.conn.amqpChannel.QueueUnbind(queueName, routingKey, exchangeName, amqp.Table(args))
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// ExchangeDelete removes the named exchange from the server. When an exchange is deleted all queue bindings
// on the exchange are also deleted. If this exchange does not exist, the channel will be closed with an error.
//
// When ifUnused is true, the server will only delete the exchange if it has no queue bindings.
// If the exchange has queue bindings the server does not delete it but close the channel with an exception instead.
// Set this to true if you are not the sole owner of the exchange.
//
// When noWait is true, do not wait for a server confirmation that the exchange has been deleted.
// Failing to delete the channel could close the channel. Add a NotifyClose listener to respond to these channel exceptions.
func (c *Consumer) RemoveExchange(name string, ifUnused bool, noWait bool) error {
	const errMessage = "failed to remove exchange: %w"

	err := c.conn.amqpChannel.ExchangeDelete(name, ifUnused, noWait)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// DecodeDeliveryBody can be used to decode the body of a delivery into v.
func (c *Consumer) DecodeDeliveryBody(delivery Delivery, v any) error {
	const errMessage = "failed to decode delivery body: %w"

	if err := c.conn.options.Codec.Decoder(delivery.Body, v); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Consumer) startConsuming() error {
	const errMessage = "failed to start consuming: %w"

	err := declareExchange(c.conn.amqpChannel, c.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = declareQueue(c.conn.amqpChannel, c.options.QueueOptions)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = declareBindings(c.conn.amqpChannel, c.options)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	deliveries, err := c.conn.amqpChannel.Consume(
		c.options.QueueOptions.name,
		c.options.ConsumerOptions.Name,
		c.options.ConsumerOptions.AutoAck,
		c.options.ConsumerOptions.Exclusive,
		false, // not supported by RabbitMQ
		c.options.ConsumerOptions.NoWait,
		amqp.Table(c.options.ConsumerOptions.Args),
	)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	for i := 0; i < c.options.HandlerQuantity; i++ {
		go c.handlerRoutine(deliveries, c.options, c.handler)
	}

	c.conn.logger.logDebug(fmt.Sprintf("Processing messages on %d message handlers", c.options.HandlerQuantity))

	return nil
}

func (c *Consumer) handlerRoutine(deliveries <-chan amqp.Delivery, consumeOptions *ConsumeOptions, handler HandlerFunc) {
	for msg := range deliveries {
		if c.conn.amqpChannel.IsClosed() {
			c.conn.logger.logDebug("message handler stopped: channel is closed")

			break
		}

		if consumeOptions.ConsumerOptions.AutoAck {
			handler(Delivery{msg})

			continue
		}

		switch handler(Delivery{msg}) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				c.conn.logger.logError("could not ack message: %v", err)
			}

		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				c.conn.logger.logError("could not nack message: %v", err)
			}

		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				c.conn.logger.logError("could not nack message: %v", err)
			}

		case Manual:
			continue
		}
	}
}

func (c *Connection) handleDelivery(delivery amqp.Delivery) {

}
