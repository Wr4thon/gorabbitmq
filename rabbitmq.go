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
	uri          string
	internalChan *amqp.Channel
	conn         *amqp.Connection
	isHealthy    *abool.AtomicBool
	isBlocked    *abool.AtomicBool
	reconnecting *abool.AtomicBool
	*sync.Mutex
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
		isHealthy:   abool.New(),
		isBlocked:   abool.New(),
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
	//rabbitMQ.blockedlistenerStop = make(chan bool)
	go rabbitMQ.connClosedListener(rabbitMQ.connlistenerStop)
	//go rabbitMQ.connBlockedListener(rabbitMQ.blockedlistenerStop)

	return &rabbitMQ, nil
}

// CheckHealth checks rabbitmq connection health
func (s *service) CheckHealth() (err error) {
	prefix := "rabbitmq healthcheck failed: "
	defer func() {
		if err != nil {
			locallog.Info(prefix, " unhealthy")
			s.isHealthy.UnSet()
		} else {
			s.isHealthy.Set()
		}
	}()
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if s.conn == nil || s.conn.IsClosed() {
		err = errors.New("rabbitmq connection is closed")
		log.Error(prefix, err)
		return err
	}
	_, err = s.internalChan.QueueDeclare("healthCheck", false, false, false, false, nil)
	if err != nil {
		//log.Error(prefix, err)
		s.internalChan, err = s.conn.Channel()
		if err != nil {
			log.Error(prefix, err)
			return err
		}
	}
	return err
}

func (s *service) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.internalChan.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s *service) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.internalChan.ExchangeBind(destination, key, source, noWait, args)
}

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.internalChan.QueueBind(name, key, exchange, noWait, args)
}

func (s *service) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.internalChan.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s *service) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	err := s.internalChan.Publish(exchange, key, mandatory, immediate, msg)
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
	go s.channelClosedListener(config)
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
				locallog.Infof(prefix+"routing message: %+v\n", delivery)
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
	s.internalChan, err = s.conn.Channel()
	if err != nil {
		log.Error(err)
		return err
	}
	for _, config := range s.ConsumerMap {
		locallog.Info(prefix, " sending stop comand to ")
		if config.Running.IsSet() {
			go func() {
				*config.stopWorkerChan <- true
			}()
		}
		locallog.Info(prefix, " restarting consumer")
		s.connectConsumerWorker(config)
		//deliveries, err := s.Consume(queueName, config.consumer, config.autoAck, config.exclusive, config.noLocal, config.noWait, config.args)
	}
	log.Info(prefix, " rabbitmq service is connected!")
	s.isHealthy.Set()
	return nil
}

func (s *service) Reconnect() error {
	return s.connect()
}

func (s *service) reconnectConsumer(config *consumerConfig) {

}

func (s *service) Close() error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if s.internalChan != nil {
		err := s.internalChan.Close()
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
					s.isBlocked.Set()
					log.Info("rabbitmq connection is blocked - too much load or ram usage on rabbitmq")
				} else {
					s.isBlocked.UnSet()
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

func (s *service) channelClosedListener(config *consumerConfig) {
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
