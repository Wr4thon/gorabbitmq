package gorabbitmq

import (
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

type RabbitMQConnector interface {
	Connect(settings QueueSettings) (RabbitMQConnection, error)
}

type rabbitMQConnector struct {
	RabbitMQConnector
}

type QueueSettings struct {
	UserName         string
	Password         string
	Host             string
	Port             int
	Durable          bool
	DeleteWhenUnused bool
	Exclusive        bool
	NoWait           bool
	QueueName        string
}

// NewRabbitMQConnector returns a new instance of the RabbitMQConnector
func NewRabbitMQConnector() RabbitMQConnector {
	return &rabbitMQConnector{}
}

func getConnectionString(queueSettings QueueSettings) string {
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

func (mq *rabbitMQConnector) Connect(settings QueueSettings) (RabbitMQConnection, error) {
	connectionString := getConnectionString(settings)

	conn, err := amqp.Dial(connectionString)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	queue, err := ch.QueueDeclare(
		settings.QueueName,
		settings.Durable,
		settings.DeleteWhenUnused,
		settings.Exclusive,
		settings.NoWait,
		nil,
	)

	if err != nil {
		return nil, err
	}

	connection := &rabbitMQConnection{
		conn:     conn,
		channel:  ch,
		queue:    queue,
		settings: settings,
	}

	return connection, nil
}
