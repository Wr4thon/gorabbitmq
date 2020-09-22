package gorabbitmq

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/Noobygames/amqp"
	"github.com/Noobygames/go-amqp-reconnect/rabbitmq"
	"github.com/pkg/errors"
)

// QueueConnector is the tcp connection for the communication with the RabbitMQ Server
// can be used to connect to a queue
type QueueConnector interface {
	ConnectToQueue(settings QueueSettings, configSetter ...ConfigBuilder) (Queue, error)
}

type queueConnector struct {
	QueueConnector
	connection      *rabbitmq.Connection
	channel         *channel
	channelSettings ChannelSettings
}

type channel struct {
	channel             *rabbitmq.Channel
	channelErrorChannel chan *amqp.Error
	closed              bool
}

// ChannelSettings are used to set prefetch count etc.
type ChannelSettings struct {
	UsePrefetch   bool // default false
	PrefetchCount int
}

func getConnectionString(queueSettings ConnectionSettings) string {
	var sb strings.Builder

	sb.WriteString("amqp://")
	if len(queueSettings.UserName) > 0 || len(queueSettings.Password) > 0 {
		sb.WriteString(url.QueryEscape(queueSettings.UserName))
		sb.WriteString(":")
		sb.WriteString(url.QueryEscape(queueSettings.Password))
		sb.WriteString("@")
	}
	sb.WriteString(url.QueryEscape(queueSettings.Host))
	sb.WriteString(":")
	sb.WriteString(strconv.Itoa(queueSettings.Port))
	sb.WriteString("/")

	return sb.String()
}

// NewConnection returns a new Instance of a tcp Connection to a RabbitMQ Server
func NewConnection(settings ConnectionSettings, channelSettings ChannelSettings) (QueueConnector, error) {
	connectionString := getConnectionString(settings)

	conn, err := rabbitmq.Dial(connectionString)

	if err != nil {
		return nil, err
	}

	connector := &queueConnector{
		connection:      conn,
		channelSettings: channelSettings,
	}

	err = connector.createChannel()
	if err != nil {
		return nil, err
	}

	return connector, err
}

func (c *queueConnector) watchChannelConnection() {
	for elem := range c.channel.channelErrorChannel {
		if c.channel.closed {
			continue
		}

		fmt.Println("recreating channel cause of error:", elem)

		err := c.createChannel()
		if err != nil {
			log.Println(err)
		}
	}
}

func (c *queueConnector) watchChannelClosed() {

}

func (c *queueConnector) createChannel() error {
	ch, err := c.connection.Channel()

	channelErrorChannel := make(chan *amqp.Error)

	c.channel = &channel{
		channel:             ch,
		channelErrorChannel: channelErrorChannel,
		closed:              false,
	}

	if err != nil {
		return err
	}

	if c.channelSettings.UsePrefetch {
		err = ch.Qos(c.channelSettings.PrefetchCount, 0, false)
		if err != nil {
			return err
		}
	}

	go c.watchChannelConnection()
	go c.watchChannelClosed()

	ch.NotifyClose(channelErrorChannel)

	return nil
}

func (c *channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (c *channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *channel) close() {
	c.closed = true
	c.channel.Close()
}

// ConnectToChannel connects to a channel
func (c *queueConnector) ConnectToQueue(queueSettings QueueSettings, configSetter ...ConfigBuilder) (Queue, error) {
	rabbitmq.Debug = true

	if c.channel == nil || c.channel.closed {
		err := c.createChannel()

		if err != nil {
			return nil, err
		}
	}

	nativeQueue, err := c.channel.channel.QueueDeclare(
		queueSettings.QueueName,
		queueSettings.Durable,
		queueSettings.DeleteWhenUnused,
		queueSettings.Exclusive,
		queueSettings.NoWait,
		nil,
	)

	if err != nil {
		return nil, err
	}

	connection := &queue{
		queueSettings: queueSettings,
		channel:       c.channel,
		queue:         nativeQueue,
	}

	for _, setter := range configSetter {
		if err := setter(connection); err != nil {
			return nil, errors.Wrap(err, "error while setting property")
		}
	}

	return connection, nil
}
