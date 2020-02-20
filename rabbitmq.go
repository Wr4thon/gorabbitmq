package gorabbitmq

import (
	"errors"
	locallog "github.com/prometheus/common/log"
	"github.com/streadway/amqp"
	"github.com/tevino/abool"
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
	CreateChannel() (*amqp.Channel, error)
}

type service struct {
	uri  string
	conn *amqp.Connection
	*sync.Mutex
	publishWrapper      *channelWrapper
	ConsumerMap         map[string]*consumerConfig
	connlistenerStop    chan bool
	blockedlistenerStop chan bool
	exchanges           map[string]exchangeConfig
	queues              map[string]queueConfig
	queueBindings       map[string]queueBindings
	MapMutex            *sync.Mutex
}

func NewRabbitMQ(settings ConnectionSettings) (RabbitMQ, error) {
	rabbitMQ := service{
		MapMutex:      &sync.Mutex{},
		Mutex:         &sync.Mutex{},
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
	rabbitMQ.connlistenerStop = make(chan bool)
	go rabbitMQ.connClosedListener(rabbitMQ.connlistenerStop)

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

	s.Mutex.Lock()
	if s.conn == nil || s.conn.IsClosed() {
		err = errors.New("rabbitmq connection is closed")
		s.Mutex.Unlock()
		return err
	}
	s.Mutex.Unlock()

	channel, err := s.createChannel()
	if channel != nil {
		_ = channel.Close()
	}

	for _, config := range s.ConsumerMap {
		if config.channel == nil {
			config.Connnected = abool.NewBool(false)
			return errors.New("consumer channel is nil")
		} else {
			queueResult, err := config.channel.QueueInspect(config.queue)
			if err != nil || queueResult.Name != config.queue {
				config.Connnected = abool.NewBool(false)
				return err
			}
		}
	}
	return err
}

func (s *service) CreateChannel() (*amqp.Channel, error) {
	return s.createChannel()
}

func (s *service) createChannel() (*amqp.Channel, error) {
	if s.conn == nil || s.conn.IsClosed() {
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

func (s *service) cleanUpChannel(channel *amqp.Channel) {
	if channel != nil {
		_ = channel.Close()
	}
}

func (s *service) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	channel, err := s.createChannel()
	if err != nil {
		return err
	}
	defer s.cleanUpChannel(channel)

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
	return channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s *service) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	channel, err := s.createChannel()
	if err != nil {
		return err
	}
	defer s.cleanUpChannel(channel)
	return channel.ExchangeBind(destination, key, source, noWait, args)
}

func (s *service) ExchangeDelete(name string, ifUnused, noWait bool) error {
	channel, err := s.createChannel()
	if err != nil {
		return err
	}
	defer s.cleanUpChannel(channel)

	if _, ok := s.exchanges[name]; ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		delete(s.exchanges, name)
	}

	return channel.ExchangeDelete(name, ifUnused, noWait)
}

func (s *service) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	channel, err := s.createChannel()
	if err != nil {
		return -1, err
	}
	defer s.cleanUpChannel(channel)

	if _, ok := s.queues[name]; ok {
		s.MapMutex.Lock()
		defer s.MapMutex.Unlock()
		delete(s.queues, name)
		_, okB := s.queueBindings[name]
		if okB {
			delete(s.queueBindings, name)
		}
	}
	return channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	channel, err := s.createChannel()
	if err != nil {
		return err
	}
	defer s.cleanUpChannel(channel)

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
	return channel.QueueBind(name, key, exchange, noWait, args)
}

func (s *service) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	channel, err := s.createChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer s.cleanUpChannel(channel)

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
	return channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s *service) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if s.publishWrapper == nil || s.publishWrapper.Connnected == nil || !s.publishWrapper.Connnected.IsSet() {
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
		stopWorkerChan:   nil,
		Connnected:       abool.NewBool(false),
	}

	config.channelWrapper = channelWrapper
	s.Mutex.Lock()
	s.ConsumerMap[config.queue] = &config
	if s.conn == nil || s.conn.IsClosed() {
		err := errors.New(prefix + "no connection available")
		locallog.Error(prefix, err)
		s.Mutex.Unlock()
		return (<-chan amqp.Delivery)(*channelWrapper.externalDelivery)
	}
	s.Mutex.Unlock()
	_ = s.connectConsumerWorker(&config)

	return (<-chan amqp.Delivery)(*channelWrapper.externalDelivery)
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
	quit := make(chan bool)
	config.stopWorkerChan = &quit
	locallog.Infof("starting consume worker config=%+v", *config)
	go s.consumerClosedListener(config)
	go runConsumerWorker(config)
	return nil
}

//async worker with nonblocking routing of deliveries and stop channel
func runConsumerWorker(config *consumerConfig) {
	config.Connnected.Set()
	defer func() {
		config.Connnected.UnSet()
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
		case stop, isOpen := <-*config.stopWorkerChan:
			{
				if !isOpen {
					locallog.Info(prefix, " consume worker stop channel was closed locally")
					return
				}
				if stop {
					locallog.Info(prefix, " consume worker stop command received")
					return
				}
			}
		}
	}
}

func (s *service) connect() error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	var err error
	s.conn, err = amqp.Dial(s.uri)
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}
	if s.publishWrapper == nil {
		s.publishWrapper = &channelWrapper{
			originalDelivery: nil,
			externalDelivery: nil,
			channel:          nil,
			stopWorkerChan:   nil,
			Connnected:       abool.NewBool(false),
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
	if s.publishWrapper.stopWorkerChan == nil {
		tmpChan := make(chan bool)
		s.publishWrapper.stopWorkerChan = &tmpChan
	}

	err = s.setUpTopology()
	if err != nil {
		locallog.Error(prefix, err)
		return err
	}

	go s.channelClosedListener(s.publishWrapper)
	for _, config := range s.ConsumerMap {
		if config.Connnected != nil || config.Connnected.IsSet() {
		} else {
			config.Connnected = abool.NewBool(false)
		}
		config.Connnected.UnSet()
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
	return s.connect()
}

func (s *service) Close() error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
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
	if s.blockedlistenerStop != nil {
		close(s.blockedlistenerStop)
	}
	if s.connlistenerStop != nil {
		close(s.connlistenerStop)
	}
	return nil
}

func (s *service) connClosedListener(stop chan bool) {
	graceful := false
	defer func() {
		//restart starts a new connClosedListener on the new connection
		if !graceful {
			s.connect()
		}
	}()
	ch := make(chan *amqp.Error)
	s.Mutex.Lock()
	s.conn.NotifyClose(ch)
	s.Mutex.Unlock()
	for {
		select {
		case connClosed, chanOpen := <-ch:
			{
				if !chanOpen {
					locallog.Errorf("%s connection was closed: %+v | %p | %t\n", prefix, connClosed, &chanOpen, chanOpen)
				} else {
					graceful = true
					locallog.Infof("%s connection was closed gracefully: %+v\n", prefix, connClosed)
				}
				return
			}
		case stop, isOpen := <-stop:
			{
				graceful = true
				if !isOpen {
					locallog.Info(prefix, " rabbitmq connlistener stop channel was closed locally")
				}
				if stop {
					locallog.Info(prefix, " rabbitmq connlistener stop command received")
				}
				return
			}
		}
	}

}

func (s *service) connBlockedListener(stop chan bool) {
	ch := make(chan amqp.Blocking)
	s.Mutex.Lock()
	s.conn.NotifyBlocked(ch)
	s.Mutex.Unlock()
	for {
		select {
		case connBlocked := <-ch:
			{
				//block is active
				if connBlocked.Active {
					locallog.Info(prefix, "connection is blocked - too much load or ram usage on rabbitmq")
				} else {
					locallog.Info(prefix, "connection is unblocked")

				}
			}
		case stop, isOpen := <-stop:
			{
				if !isOpen {
					locallog.Info(prefix, " rabbitmq blockedlistener stop channel was closed locally")
				}
				if stop {
					locallog.Info(prefix, " rabbitmq blockedlistener stop command received")
				}
				return
			}
		}
	}
}

func (s *service) channelClosedListener(wrapper *channelWrapper) {
	ch := make(chan *amqp.Error)
	wrapper.channel.NotifyClose(ch)
	wrapper.Connnected.Set()
	defer func() {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		wrapper.channel = nil
		wrapper.Connnected.UnSet()
	}()
	for {
		select {
		case err, open := <-ch:
			{
				//block is active
				if err != nil {
					locallog.Info(prefix, "channel disconnected: ", err)
					return
				}
				if !open {
					locallog.Info(prefix, "channel disconnected")
					return
				}
			}
		case stop, isOpen := <-*wrapper.stopWorkerChan:
			{
				if !isOpen {
					locallog.Info(prefix, "rabbitmq blockedlistener stop channel was closed locally")
				}
				if stop {
					locallog.Info(prefix, "rabbitmq blockedlistener stop command received")
				}
				return
			}
		}
	}
}

func (s *service) consumerClosedListener(config *consumerConfig) {
	ch := make(chan *amqp.Error)
	config.channel.NotifyClose(ch)
	for {
		queueChanClosed, chanOpen := <-ch
		if !chanOpen {
			locallog.Errorf("%s worker channel was closed: %+v\n", prefix, queueChanClosed)
		} else {
			locallog.Infof("%s worker channel was closed gracefully: %+v\n", prefix, queueChanClosed)
		}
		break
	}
}
