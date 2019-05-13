package gorabbitmq

import (
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

// QueueConnector is the tcp connection for the communication with the RabbitMQ Server
// can be used to connect to a queue
type QueueConnector interface {
	ConnectToQueue(settings QueueSettings) (Queue, error)
}

type queueConnector struct {
	QueueConnector
	connection *amqp.Connection
	channel    *amqp.Channel
}

func getConnectionString(queueSettings ConnectionSettings) string {
	var sb strings.Builder

	sb.WriteString("amqp://")
	sb.WriteString(queueSettings.UserName)
	sb.WriteString(":")
	sb.WriteString(queueSettings.Password)
	sb.WriteString("@")
	sb.WriteString(queueSettings.Host)
	sb.WriteString(":")
	sb.WriteString(strconv.Itoa(queueSettings.Port))
	sb.WriteString("/")

	return sb.String()
}

// NewConnection returns a new Instance of a tcp Connection to a RabbitMQ Server
func NewConnection(settings ConnectionSettings) (QueueConnector, error) {
	connectionString := getConnectionString(settings)

	conn, err := amqp.Dial(connectionString)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	return &queueConnector{
		connection: conn,
		channel:    ch,
	}, err
}

// ConnectToChannel connects to a channel
func (c *queueConnector) ConnectToQueue(queueSettings QueueSettings) (Queue, error) {
	nativeQueue, err := c.channel.QueueDeclare(
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

	return connection, nil
}
