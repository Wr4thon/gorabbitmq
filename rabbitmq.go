package gorabbitmq

import (
	"errors"
	"github.com/labstack/gommon/log"
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
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) <-chan amqp.Delivery
	Reconnect() error
}

type service struct {
	uri  string
	conn *amqp.Connection
	*sync.Mutex
	publishChan         *amqp.Channel
	publishStop         chan bool
	ConsumerMap         map[string]*consumerConfig
	connlistenerStop    chan bool
	blockedlistenerStop chan bool
}

type consumerConfig struct {
	queue, consumer                     string
	autoAck, exclusive, noLocal, noWait bool
	args                                amqp.Table
	channelWrapper
}

type channelWrapper struct {
	originalDelivery *<-chan amqp.Delivery
	externalDelivery *chan amqp.Delivery
	queueChan        *amqp.Channel
	stopWorkerChan   *chan bool
	Running          abool.AtomicBool
}

func NewRabbitMQ(settings ConnectionSettings) (RabbitMQ, error) {
	rabbitMQ := service{
		Mutex:       &sync.Mutex{},
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
		log.Error(err)
		return &rabbitMQ, err
	}
	rabbitMQ.connlistenerStop = make(chan bool)
	go rabbitMQ.connClosedListener(rabbitMQ.connlistenerStop)
	//go rabbitMQ.connBlockedListener(rabbitMQ.blockedlistenerStop) not needed for now

	return &rabbitMQ, nil
}

// checks rabbitmq connection health
func (s *service) CheckHealth() (err error) {
	prefix := "rabbitmq healthcheck failed: "
	defer func() {
		if err != nil {
			locallog.Info(prefix, " unhealthy")
		}
	}()
	s.Mutex.Lock()
	if s.conn == nil || s.conn.IsClosed() {
		err = errors.New("rabbitmq connection is closed")
		log.Error(prefix, err)
		s.Mutex.Unlock()
		return err
	}
	s.Mutex.Unlock()

	channel, err := s.createChannel()
	if channel != nil {
		_ = channel.Close()
	}
	return err
}

func (s *service) createChannel() (*amqp.Channel, error) {
	if s.conn == nil || s.conn.IsClosed() {
		err := errors.New(prefix + "no connection available")
		log.Error(prefix, err)
		return nil, err
	}
	channel, err := s.conn.Channel()
	if err != nil {
		log.Error(prefix, err)
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

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	channel, err := s.createChannel()
	if err != nil {
		return err
	}
	defer s.cleanUpChannel(channel)
	return channel.QueueBind(name, key, exchange, noWait, args)
}

func (s *service) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	channel, err := s.createChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer s.cleanUpChannel(channel)
	return channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s *service) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	err := s.publishChan.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		log.Error(prefix, err)
		return err
	}
	return nil
}

//create consume on rabbitmq which is valid after service reconnects
func (s *service) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) <-chan amqp.Delivery {
	config := consumerConfig{
		queue:     queue,
		noWait:    noWait,
		noLocal:   noLocal,
		exclusive: exclusive,
		autoAck:   autoAck,
		consumer:  consumer,
		args:      args,
	}

	externalDelivery := make(chan amqp.Delivery)
	quit := make(chan bool)
	channelWrapper := channelWrapper{
		originalDelivery: nil,
		externalDelivery: &externalDelivery,
		queueChan:        nil,
		stopWorkerChan:   &quit,
	}

	config.channelWrapper = channelWrapper
	s.Mutex.Lock()
	s.ConsumerMap[config.queue] = &config
	s.Mutex.Unlock()
	s.connectConsumerWorker(&config)
	return (<-chan amqp.Delivery)(*channelWrapper.externalDelivery)
}

func (s *service) connectConsumerWorker(config *consumerConfig) {
	queueChan, err := s.conn.Channel()
	if err != nil {
		log.Error(prefix, err)
	}
	chanDeliveries, err := queueChan.Consume(config.queue, config.consumer, config.autoAck, config.exclusive, config.noLocal, config.noWait, config.args)
	if err != nil {
		log.Error(prefix, err)
	}
	config.channelWrapper.queueChan = queueChan
	if chanDeliveries != nil {
		config.channelWrapper.originalDelivery = &chanDeliveries
	} else {
		return
	}
	go s.consumerClosedListener(config)
	go runConsumerWorker(config)
}

//async worker with nonblocking routing of deliveries and stop channel
func runConsumerWorker(config *consumerConfig) {
	config.Running.Set()
	defer func() {
		config.Running.UnSet()
		if err := config.queueChan.Close(); err != nil {
			log.Error(prefix, err)
		}
	}()
	for {
		select {
		case delivery, isOpen := <-*config.originalDelivery:
			{
				if !isOpen {
					log.Info(prefix, "consume worker amqp.Delivery channel was closed by rabbitmq server")
					return
				}
				//route message through
				*config.externalDelivery <- delivery
			}
		case stop, isOpen := <-*config.stopWorkerChan:
			{
				if !isOpen {
					log.Info(prefix, " consume worker stop channel was closed locally")
					return
				}
				if stop {
					log.Info(prefix, " consume worker stop command received")
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
		log.Error(err)
		return err
	}
	s.publishChan, err = s.conn.Channel()
	if err != nil {
		log.Error(err)
		return err
	}
	if s.publishStop == nil {
		s.publishStop = make(chan bool)
	}
	go s.channelClosedListener(s.publishChan, s.publishStop)
	for _, config := range s.ConsumerMap {
		locallog.Info(prefix, " sending stop comand to ")
		if config.Running.IsSet() {
			go func() {
				*config.stopWorkerChan <- true
			}()
		}
		locallog.Info(prefix, " restarting consumer")
		s.connectConsumerWorker(config)
	}
	log.Info(prefix, " rabbitmq service is connected!")
	return nil
}

func (s *service) Reconnect() error {
	return s.connect()
}

func (s *service) Close() error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
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
					log.Errorf("rabbitmq connection was closed: %+v | %p | %b\n", connClosed, &chanOpen, chanOpen)
				} else {
					graceful = true
					log.Info("rabbitmq connection was closed gracefully: %+v\n", connClosed)
				}
				return
			}
		case stop, isOpen := <-stop:
			{
				graceful = true
				if !isOpen {
					log.Info(prefix, " rabbitmq connlistener stop channel was closed locally")
				}
				if stop {
					log.Info(prefix, " rabbitmq connlistener stop command received")
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
					log.Info("rabbitmq connection is blocked - too much load or ram usage on rabbitmq")
				} else {
					log.Info("rabbitmq connection is unblocked")

				}
			}
		case stop, isOpen := <-stop:
			{
				if !isOpen {
					log.Info(prefix, " rabbitmq blockedlistener stop channel was closed locally")
				}
				if stop {
					log.Info(prefix, " rabbitmq blockedlistener stop command received")
				}
				return
			}
		}
	}
}

func (s *service) channelClosedListener(channel *amqp.Channel, quit chan bool) {
	ch := make(chan *amqp.Error)
	channel.NotifyClose(ch)
	defer func() {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		s.publishChan = nil
	}()
	for {
		select {
		case err, open := <-ch:
			{
				//block is active
				if err != nil {
					log.Info("rabbitmq channel disconnected")
					return
				}
				if !open {
					log.Info("rabbitmq channel disconnected")
					return
				}
			}
		case stop, isOpen := <-quit:
			{
				if !isOpen {
					log.Info(prefix, " rabbitmq blockedlistener stop channel was closed locally")
				}
				if stop {
					log.Info(prefix, " rabbitmq blockedlistener stop command received")
				}
				return
			}
		}
	}
}

func (s *service) consumerClosedListener(config *consumerConfig) {
	ch := make(chan *amqp.Error)
	config.queueChan.NotifyClose(ch)
	graceful := false
	for {
		queueChanClosed, chanOpen := <-ch
		if !chanOpen {
			log.Errorf("rabbitmq worker channel was closed: %+v\n", queueChanClosed)
		} else {
			graceful = true
			log.Info("rabbitmq worker channel was closed gracefully: %+v\n", queueChanClosed)
		}
		break
	}
	if !graceful {
		//TODO impl
	}
}
