package gorabbitmq

import (
	"errors"
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
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) <-chan amqp.Delivery
	Reconnect() error
	CreateChannel() (*rabbitmq.Channel, error)
}

type service struct {
	uri  string
	conn *rabbitmq.Connection
	*sync.RWMutex
	publishWrapper *channelWrapper
	ConsumerMap    map[string]*consumerConfig
	exchanges      map[string]exchangeConfig
	queues         map[string]queueConfig
	queueBindings  map[string]queueBindings
	MapMutex       *sync.Mutex
}

func NewRabbitMQ(settings ConnectionSettings) (RabbitMQ, error) {
	rabbitMQ := service{
		MapMutex:      &sync.Mutex{},
		RWMutex:       &sync.RWMutex{},
		ConsumerMap:   map[string]*consumerConfig{},
		exchanges:     map[string]exchangeConfig{},
		queues:        map[string]queueConfig{},
		queueBindings: map[string]queueBindings{},
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
			locallog.Error(prefix, " unhealthy reason=", err)
		}
	}()
	s.RWMutex.RLock()
	if s.conn == nil {
		err = errors.New("rabbitmq connection is closed")
		s.RWMutex.RUnlock()
		return err
	}
	s.RWMutex.RUnlock()

	channel, err := s.createChannel()
	if channel != nil {
		_ = channel.Close()
	}

	for _, config := range s.ConsumerMap {
		if config.channel == nil {
			return errors.New("consumer channel is nil")
		} else {
			queueResult, err := config.channel.QueueInspect(config.queue)
			if err != nil || queueResult.Name != config.queue {
				return err
			}
		}
	}
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
	if s.publishWrapper.channel == nil {
		return errors.New("no channel available")
	}
	if _, ok := s.exchanges[name]; !ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		s.exchanges[name] = exchangeConfig{
			name:       name,
			kind:       kind,
			durable:    durable,
			autoDelete: autoDelete,
			internal:   internal,
			noWait:     noWait,
			args:       args,
		}
	}
	return s.publishWrapper.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s *service) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	if s.publishWrapper.channel == nil {
		return errors.New("no channel available")
	}
	return s.publishWrapper.channel.ExchangeBind(destination, key, source, noWait, args)
}

func (s *service) ExchangeDelete(name string, ifUnused, noWait bool) error {
	if s.publishWrapper.channel == nil {
		return errors.New("no channel available")
	}
	if _, ok := s.exchanges[name]; ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		delete(s.exchanges, name)
	}

	return s.publishWrapper.channel.ExchangeDelete(name, ifUnused, noWait)
}

func (s *service) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	if s.publishWrapper.channel == nil {
		return -1, errors.New("no channel available")
	}

	if _, ok := s.queues[name]; ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		delete(s.queues, name)
		_, okB := s.queueBindings[name]
		if okB {
			delete(s.queueBindings, name)
		}
	}
	return s.publishWrapper.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if s.publishWrapper.channel == nil {
		return errors.New("no channel available")
	}

	if _, ok := s.queueBindings[name]; !ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		s.queueBindings[name] = queueBindings{
			args:     args,
			noWait:   noWait,
			name:     name,
			key:      key,
			exchange: exchange,
		}
	}
	return s.publishWrapper.channel.QueueBind(name, key, exchange, noWait, args)
}

func (s *service) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if s.publishWrapper.channel == nil {
		return amqp.Queue{}, errors.New("no channel available")
	}

	if _, ok := s.queues[name]; !ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		s.queues[name] = queueConfig{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
			exclusive:  exclusive,
			noWait:     noWait,
			args:       args,
		}
	}
	return s.publishWrapper.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s *service) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if s.publishWrapper == nil {
		err := errors.New("no connection for publish available")
		locallog.Error(prefix, err)
		return err
	}
	err := s.publishWrapper.channel.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}
	return nil
}

//create consume on rabbitmq which is valid after service reconnects
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
	s.RWMutex.Lock()
	s.ConsumerMap[config.queue] = &config
	if s.conn == nil {
		err := errors.New(prefix + "no connection available")
		locallog.Error(prefix, err)
		s.RWMutex.Unlock()
		return *channelWrapper.externalDelivery
	}
	s.RWMutex.Unlock()
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
	defer func() {
		if err := config.channel.Close(); err != nil {
			locallog.Error(prefix, err)
		}
	}()
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
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
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
	if s.publishWrapper == nil {
		s.publishWrapper = &channelWrapper{
			originalDelivery: nil,
			externalDelivery: nil,
			channel:          nil,
		}
	}

	publishChan, err := s.conn.Channel()
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}
	s.publishWrapper.channel = publishChan
	err = publishChan.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		locallog.Error(prefix, err)
	}

	err = s.setUpTopology()
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}

	for _, config := range s.ConsumerMap {
		locallog.Info(prefix, " restarting consumer")
		if err = s.connectConsumerWorker(config); err != nil {
			return err
		}
	}
	locallog.Info(prefix, " rabbitmq service is connected!")
	return nil
}

func (s *service) setUpTopology() error {
	for _, config := range s.exchanges {
		err := s.ExchangeDeclare(config.name, config.kind, config.durable, config.autoDelete, config.internal, config.noWait, config.args)
		if err != nil {
			return err
		}
	}
	for _, config := range s.queues {
		_, err := s.QueueDeclare(config.name, config.durable, config.autoDelete, config.exclusive, config.noWait, config.args)
		if err != nil {
			return err
		}
	}
	for _, config := range s.queueBindings {
		err := s.QueueBind(config.name, config.key, config.exchange, config.noWait, config.args)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) Reconnect() error {
	return nil
}

func (s *service) Close() error {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if s.publishWrapper != nil && s.publishWrapper.channel != nil {
		err := s.publishWrapper.channel.Close()
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
