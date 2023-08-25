package gorabbitmq

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	closeWGDelta int = 2

	reconnectFailChanSize int = 32
)

type Connection struct {
	options *ConnectionOptions

	amqpConnection *amqp.Connection
	amqpChannel    *amqp.Channel

	connectionCloseWG *sync.WaitGroup

	startRecoveryChan  chan struct{}
	recoveryFailedChan chan error

	recoverConsumerChan   chan struct{}
	consumerRecoveredChan chan error
	consumerSubscribed    bool

	closeChan chan struct{}

	logger *log

	returnHandler ReturnHandler
}

// NewConnection creates a new connection.
//
// Needs to be closed with the Close() method.
func NewConnection(settings *ConnectionSettings, options ...ConnectionOption) (*Connection, error) {
	const errMessage = "failed to create connection %w"

	opt := defaultConnectionOptions(
		fmt.Sprintf("amqp://%s:%s@%s/",
			url.QueryEscape(settings.UserName),
			url.QueryEscape(settings.Password),
			net.JoinHostPort(
				url.QueryEscape(settings.Host),
				strconv.Itoa(settings.Port),
			),
		),
	)

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	conn := &Connection{
		connectionCloseWG:     &sync.WaitGroup{},
		startRecoveryChan:     make(chan struct{}),
		recoveryFailedChan:    make(chan error, reconnectFailChanSize),
		recoverConsumerChan:   make(chan struct{}),
		consumerRecoveredChan: make(chan error),
		closeChan:             make(chan struct{}),
		logger:                newLogger(opt.logger),
		returnHandler:         opt.ReturnHandler,
		options:               opt,
	}

	err := conn.connect()
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return conn, nil
}

// Close gracefully closes the connection to the server.
func (c *Connection) Close() error {
	const errMessage = "failed to close connection to rabbitmq gracefully: %w"

	if c.amqpConnection != nil {
		c.logger.logDebug("closing connection")

		if c.consumerSubscribed {
			c.closeChan <- struct{}{}
		}

		c.connectionCloseWG.Add(closeWGDelta)

		err := c.amqpConnection.Close()

		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.connectionCloseWG.Wait()

		close(c.startRecoveryChan)
		close(c.recoveryFailedChan)
		close(c.recoverConsumerChan)
		close(c.consumerRecoveredChan)
		close(c.closeChan)
	}

	c.logger.logInfo("gracefully closed connection to rabbitmq")

	return nil
}

func (c *Connection) NotifyRecoveryFail() (<-chan error, error) {
	return c.recoveryFailedChan, nil
}

// // Reconnect can be used to manually reconnect to a RabbitMQ.
// //
// // Returns an error if the current connection persists.
// func (c *Connection) Reconnect() error {
// 	const errMessage = "failed to reconnect to rabbitmq: %w"

// 	if c.amqpConnection != nil {
// 		return fmt.Errorf(errMessage, ErrHealthyConnection)
// 	}

// 	// c.watchReconnectionFailes()

// 	err := c.startRecovery()
// 	if err != nil {
// 		return fmt.Errorf(errMessage, err)
// 	}

// 	return nil
// }

func (c *Connection) connect() error {
	const errMessage = "failed to connect to rabbitmq: %w"

	if c.amqpConnection == nil {
		err := c.createConnection()
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		err = c.createChannel()
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.watchRecoveryChan()
	}

	return nil
}

func (c *Connection) createConnection() error {
	const errMessage = "failed to create channel: %w"

	var err error

	c.amqpConnection, err = amqp.DialConfig(c.options.uri, amqp.Config(*c.options.Config))
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.watchConnectionNotifications()

	return nil
}

func (c *Connection) createChannel() error {
	const errMessage = "failed to create channel: %w"

	var err error

	c.amqpChannel, err = c.amqpConnection.Channel()
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if c.options.PrefetchCount > 0 {
		err = c.amqpChannel.Qos(c.options.PrefetchCount, 0, false)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	c.watchChannelNotifications()

	return nil
}

func (c *Connection) watchConnectionNotifications() {
	closeChan := c.amqpConnection.NotifyClose(make(chan *amqp.Error))
	blockChan := c.amqpConnection.NotifyBlocked(make(chan amqp.Blocking))

	go func() {
		for {
			select {
			case err := <-closeChan:
				if err == nil {
					slog.Debug("closed connection")

					c.connectionCloseWG.Done()

					return
				}

				c.logger.logDebug("connection unexpectedly closed", "cause", err)

				c.startRecoveryChan <- struct{}{}

				return

			case block := <-blockChan:
				c.logger.logWarn("connection exception", "cause", block.Reason)
			}
		}
	}()
}

func (c *Connection) watchChannelNotifications() {
	closeChan := c.amqpChannel.NotifyClose(make(chan *amqp.Error))
	cancelChan := c.amqpChannel.NotifyCancel(make(chan string))
	returnChan := c.amqpChannel.NotifyReturn(make(chan amqp.Return))

	go func() {
		for {
			select {
			case err := <-closeChan:
				if err == nil {
					slog.Debug("closed channel")

					c.connectionCloseWG.Done()

					return
				}

				c.logger.logDebug("channel unexpectedly closed", "cause", err)

				return

			case tag := <-cancelChan:
				c.logger.logWarn("cancel exception", "cause", tag)

			case rtrn := <-returnChan:
				if c.returnHandler != nil {
					c.returnHandler(Return(rtrn))

					continue
				}

				c.logger.logWarn(
					"message could not be published",
					"replyCode", rtrn.ReplyCode,
					"replyText", rtrn.ReplyText,
					"messageId", rtrn.MessageId,
					"correlationId", rtrn.CorrelationId,
					"exchange", rtrn.Exchange,
					"routingKey", rtrn.RoutingKey,
				)
			}
		}
	}()
}

func (c *Connection) watchRecoveryChan() {
	go func() {
		for range c.startRecoveryChan {
			err := c.startRecovery()
			if err != nil {
				c.recoveryFailedChan <- err

				return
			}
		}
	}()
}

func (c *Connection) startRecovery() error {
	const errMessage = "recovery failed: %w"

	err := c.backoff(
		func() error {
			err := c.createConnection()
			if err != nil {
				return err
			}

			err = c.createChannel()
			if err != nil {
				return err
			}

			return nil
		},
	)
	if err != nil {
		c.amqpConnection = nil
		c.amqpChannel = nil

		return fmt.Errorf(errMessage, err)
	}

	if c.consumerSubscribed {
		err = c.recoverConsumer()
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	c.logger.logInfo("successfully recovered connection")

	return nil
}

func (c *Connection) recoverConsumer() error {
	const errMessage = "failed to recover consumer %w"

	c.recoverConsumerChan <- struct{}{}

	err := <-c.consumerRecoveredChan
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.logger.logDebug("successfully recovered consumer")

	return nil
}

func (c *Connection) backoff(action func() error) error {
	const errMessage = "backoff failed %w"

	retry := 0

	for retry <= c.options.MaxReconnectRetries {
		if action() == nil {
			c.logger.logDebug("successfully reestablished amqp-connection", "retries", retry)

			break
		}

		if retry == c.options.MaxReconnectRetries {
			c.logger.logDebug("reconnection failed: maximum retries exceeded", "retries", retry)

			return fmt.Errorf(errMessage, ErrMaxRetriesExceeded)
		}

		delay := time.Duration(c.options.BackoffFactor*retry) * c.options.ReconnectInterval

		c.logger.logDebug("failed to reconnect: backing off...", "backoff-time", delay.String())

		time.Sleep(delay)

		retry++
	}

	return nil
}
