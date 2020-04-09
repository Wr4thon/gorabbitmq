package gorabbitmq

import (
	"errors"
	"fmt"
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	locallog "github.com/prometheus/common/log"
	"github.com/streadway/amqp"
	"sync"
)

const prefix = "rabbitmq-lib "

type RabbitMQ interface {
	Close() error
	CheckHealth() (err error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) <-chan amqp.Delivery //deprecated
	Reconnect() error                                                                                                                                //deprecated
	CreateChannel() (*rabbitmq.Channel, error)
	ConsumeQos(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithChannel(channel *rabbitmq.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

type service struct {
	uri         string
	pubMutex    sync.Mutex
	publishChan *rabbitmq.Channel
	conn        *rabbitmq.Connection
	ConsumerMap map[string]*consumerConfig
}

func NewRabbitMQ(settings ConnectionSettings) (RabbitMQ, error) {
	rabbitMQ := service{
		ConsumerMap: map[string]*consumerConfig{},
	}

	uri := amqp.URI{
		Host:     settings.Host,
		Username: settings.UserName,
		Password: settings.Password,
		Port:     settings.Port,
		Vhost:    "/",
		Scheme:   "amqp",
	}

	rabbitMQ.uri = uri.String()
	if err := rabbitMQ.connect(); err != nil {
		locallog.Error(prefix, err)
		return &rabbitMQ, err
	}

	return &rabbitMQ, nil
}

// checks rabbitmq connection health
func (s *service) CheckHealth() (err error) {
	prefix := "rabbitmq healthcheck: "
	defer func() {
		if err != nil {
			err = errors.New(prefix + err.Error())
		}
	}()
	if s.conn == nil {
		err = errors.New("rabbitmq connection is closed")
		return err
	}

	channel, err := s.conn.Channel()
	if channel == nil {
		err = errors.New(fmt.Sprint("rabbitmq created channel failed:", err))
		return err
	}
	defer channel.Close()
	return err
}

func (s *service) CreateChannel() (*rabbitmq.Channel, error) {
	return s.createChannel()
}

func (s *service) createChannel() (*rabbitmq.Channel, error) {
	if s.conn == nil {
		err := errors.New(prefix + "no connection available")
		locallog.Error(prefix, err)
		return nil, err
	}
	channel, err := s.conn.Channel()
	if err != nil {
		locallog.Error(prefix, err)
		return channel, err
	}

	return channel, err
}

func (s *service) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	channel, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s *service) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	channel, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.ExchangeBind(destination, key, source, noWait, args)
}

func (s *service) ExchangeDelete(name string, ifUnused, noWait bool) error {
	channel, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.ExchangeDelete(name, ifUnused, noWait)
}

func (s *service) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	channel, err := s.conn.Channel()
	if err != nil {
		return -1, err
	}
	defer channel.Close()
	return channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	channel, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.QueueBind(name, key, exchange, noWait, args)
}

func (s *service) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	channel, err := s.conn.Channel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer channel.Close()
	return channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s *service) PublishWithChannel(channel *rabbitmq.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if channel == nil {
		return errors.New("channel is nil")
	}
	err := channel.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}
	return nil
}

func (s *service) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil {
		channel, err := s.conn.Channel()
		if err != nil {
			return err
		}
		s.publishChan = channel
	}
	err := s.publishChan.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}
	return nil
}

func (s *service) ConsumeQos(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) (<-chan amqp.Delivery, error) {
	channel, err := s.conn.Channel()
	if err != nil {
		locallog.Error(err)
		return nil, err
	}
	if err = channel.Qos(prefetchCount, prefetchSize, false); err != nil {
		return nil, err
	}
	return channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

//deprecated
func (s *service) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) <-chan amqp.Delivery {
	config := consumerConfig{
		queue:         queue,
		noWait:        noWait,
		noLocal:       noLocal,
		exclusive:     exclusive,
		autoAck:       autoAck,
		consumer:      consumer,
		args:          args,
		prefetchCount: prefetchCount,
		prefetchSize:  prefetchSize,
	}
	externalDelivery := make(chan amqp.Delivery)
	channelWrapper := channelWrapper{
		originalDelivery: nil,
		externalDelivery: &externalDelivery,
		channel:          nil,
	}

	config.channelWrapper = channelWrapper
	s.ConsumerMap[config.queue] = &config
	if s.conn == nil {
		err := errors.New(prefix + "no connection available")
		locallog.Error(prefix, err)
		return *channelWrapper.externalDelivery
	}
	_ = s.connectConsumerWorker(&config)

	return *channelWrapper.externalDelivery
}

func (s *service) connectConsumerWorker(config *consumerConfig) (err error) {
	queueChan, err := s.conn.Channel()
	if err != nil {
		locallog.Error(prefix, err)
		return
	}
	err = queueChan.Qos(config.prefetchCount, config.prefetchSize, false)
	if err != nil {
		locallog.Error(prefix, err)
		return
	}
	chanDeliveries, err := queueChan.Consume(config.queue, config.consumer, config.autoAck, config.exclusive, config.noLocal, config.noWait, config.args)
	if err != nil {
		locallog.Error(prefix, err)
		return
	}
	config.channelWrapper.channel = queueChan
	if chanDeliveries != nil {
		config.channelWrapper.originalDelivery = &chanDeliveries
	} else {
		return
	}
	locallog.Infof("starting consume worker config=%+v", *config)
	go runConsumerWorker(config)
	return nil
}

//async worker with nonblocking routing of deliveries and stop channel
func runConsumerWorker(config *consumerConfig) {
	for {
		select {
		case delivery, isOpen := <-*config.originalDelivery:
			{
				if !isOpen {
					locallog.Info(prefix, "consume worker amqp.Delivery channel was closed by rabbitmq server")
					return
				}
				//route message through
				*config.externalDelivery <- delivery
			}
		}
	}
}

func (s *service) connect() error {
	var err error

	if s.conn != nil && !s.conn.IsClosed() {
		if err := s.conn.Close(); err != nil {
			locallog.Error(err)
		}
	}

	s.conn, err = rabbitmq.Dial(s.uri)
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}

	publishChan, err := s.conn.Channel()
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}
	s.publishChan = publishChan
	err = publishChan.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		locallog.Error(prefix, err)
	}

	locallog.Info(prefix, " rabbitmq service is connected!")
	return nil
}

func (s *service) Reconnect() error {
	return nil
}

func (s *service) Close() error {
	if s.publishChan != nil {
		err := s.publishChan.Close()
		if err != nil {
			return err
		}
	}
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
