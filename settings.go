package gorabbitmq

import (
	"github.com/streadway/amqp"
	"github.com/tevino/abool"
)

// ConnectionSettings holds settings for a rabbitMQConnector connection
type ConnectionSettings struct {
	// UserName containes the username of the rabbitMQ user
	UserName string `json:"userName,omitempty"`
	// Password containes the password of the rabbitMQ user
	Password string `json:"password,omitempty"`
	// Host containes the hostname or ip of the rabbitMQ server
	Host string `json:"host,omitempty"`
	// Post containes the port number the rabbitMQ server is listening on
	Port int `json:"port,omitempty"`
}

type exchangeConfig struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

type queueConfig struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
}

type queueBindings struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     amqp.Table
}

type consumerConfig struct {
	prefetchCount, prefetchSize         int
	queue, consumer                     string
	autoAck, exclusive, noLocal, noWait bool
	args                                amqp.Table
	channelWrapper
}

type channelWrapper struct {
	originalDelivery *<-chan amqp.Delivery
	externalDelivery *chan amqp.Delivery
	channel          *amqp.Channel
	stopWorkerChan   *chan bool
	Connnected       *abool.AtomicBool
}
