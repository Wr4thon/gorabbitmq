package gorabbitmq

import (
	"context"
	"errors"
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"github.com/tevino/abool"
	"sync"
	"time"
)

type ConsumerHook func(context.Context) error
type ReconnectPolicy func(retry int) time.Duration

const prefix = "rabbitmq-lib "

type RabbitMQ interface {
	Close() error
	connClosedListener()
	connBlockedListener()
	CheckHealth() (err error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	ConnectToQueue(queueSettings QueueSettings) (Queue, error)
}

type service struct {
	uri          string
	internalChan *amqp.Channel
	conn         *amqp.Connection
	isHealthy    *abool.AtomicBool
	isBlocked    *abool.AtomicBool
	reconnecting *abool.AtomicBool
	*sync.Mutex
	channelMap sync.Map
}

type channel struct {
	channel *amqp.Channel
	queue
}

func NewRabbitMQ(settings ConnectionSettings) (RabbitMQ, error) {
	rabbitMQ := service{
		isHealthy:  abool.New(),
		isBlocked:  abool.New(),
		Mutex:      &sync.Mutex{},
		channelMap: sync.Map{},
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
		return nil, err
	}

	go rabbitMQ.connClosedListener()
	go rabbitMQ.connBlockedListener()

	return &rabbitMQ, nil
}

// CheckHealth checks rabbitmq connection health
func (s *service) CheckHealth() (err error) {
	prefix := "rabbitmq healthcheck failed: "
	defer func() {
		if err != nil {
			s.isHealthy.UnSet()
		}
	}()
	if s.conn.IsClosed() {
		err = errors.New("rabbitmq connection is closed")
		log.Error(prefix, err)
		return err
	}
	_, err = s.internalChan.QueueDeclare("healthCheck", false, false, false, false, nil)
	if err != nil {
		log.Error(prefix, err)
	}

	err = s.internalChan.ExchangeDeclarePassive("amqp.direct", "direct", false, false, false, false, nil)
	if err != nil {
		log.Error(prefix, err)
		return err
	}
	return err
}

func (s *service) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return s.internalChan.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s *service) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	return s.internalChan.ExchangeBind(destination, key, source, noWait, args)
}

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return s.internalChan.QueueBind(name, key, exchange, noWait, args)
}

// ConnectToChannel connects to a channel
func (s *service) ConnectToQueue(queueSettings QueueSettings) (Queue, error) {
	qChannel, err := s.conn.Channel()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	nativeQueue, err := qChannel.QueueDeclare(
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

	queue := &queue{
		queueSettings: queueSettings,
		channel:       qChannel,
		queue:         nativeQueue,
	}
	s.channelMap.Store(queue.queueSettings.QueueName, qChannel)
	return queue, nil
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
	return err
}

func (s *service) Reconnect() error {
	return s.connect()
}

func (s *service) connClosedListener() {
	ch := make(chan *amqp.Error)
	s.Mutex.Lock()
	s.conn.NotifyClose(ch)
	s.Mutex.Unlock()
	graceful := false
	for {
		connClosed, chanOpen := <-ch
		if !chanOpen {
			log.Errorf("rabbitmq connection was closed: %+v\n", connClosed)
			close(ch)
		} else {
			graceful = true
			log.Info("rabbitmq connection was closed gracefully")
		}
		break
	}
	//restart starts a new connClosedListener on the new connection
	if !graceful {
		s.connect()
	}
}

func (s *service) connBlockedListener() {
	ch := make(chan amqp.Blocking)
	s.Mutex.Lock()
	s.conn.NotifyBlocked(ch)
	s.Mutex.Unlock()
	for {
		connBlocked := <-ch
		//block is active
		if connBlocked.Active {
			s.isBlocked.Set()
			log.Info("rabbitmq connection is blocked - too much load or ram usage on rabbitmq")
		} else {
			s.isBlocked.UnSet()
			log.Info("rabbitmq connection is unblocked")

		}
	}
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
	return nil
}
