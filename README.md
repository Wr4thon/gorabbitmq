# WIP

please don't use this yet, as I have to change some things around. Naming is still not final and will change in the future.

This Lib is a wrapper around the Go RabbitMQ Client Library. https://github.com/streadway/amqp

Supported Go Versions

This library supports two most recent Go, currently 1.13

## INSTALL 

```bash
go get github.com/Wr4thon/gorabbitmq
```

## USAGE

### Initialisation:

```Go
connectionSettings := gorabbitmq.ConnectionSettings{
		UserName: rabbitMQUser,
		Password: rabbitMQPassword,
		Host:     rabbitMQHost,
		Port:     port,
}
channelSettings := gorabbitmq.channelSettings{
	usePrefetch:   false
	prefetchCount: 1
}

qConnector, err := gorabbitmq.NewConnection(connectionSettings,channelSettings)
if err != nil {
  return nil, errors.Wrap(err, "could not connect to new queue")
}

queueSettings := gorabbitmq.QueueSettings{
  QueueName: queueName, // "ExampleQueueName"
  Durable:          true,
  DeleteWhenUnused: false,
  Exclusive:        false,
  NoWait:           false,
}

q, err := qConnector.ConnectToQueue(queueSettings)
if err != nil {
  return nil, errors.Wrap(err, "could not connect to queue")
}
return q, nil
```

### Enqeue:

```Go
err := q.Send(queueRequest)
if err != nil {
	return errors.Wrap(err, "could not publish article")
}
return nil
```

### Consume:

```Go
consumerSettings := gorabbitmq.ConsumerSettings{AutoAck: false, Exclusive: false, NoLocal: false, NoWait: false}

var request models.Request
fn := func(delivery amqp.Delivery) error {
	if service.stopConsuming {
		err := delivery.Nack(false, true)
		if err != nil {
			return errors.Wrap(err, "could not Nack")
		}
		return nil
	}

	if service.queue.IsClosed() {
		return errors.New("queue channel was closed")
	}

	err := json.Unmarshal(delivery.Body, &request)
	if err != nil {
		nackErr := delivery.Nack(false, false)
		if nackErr != nil {
			return errors.Wrap(err, "could not Nack while unmarshall error")
		}


		return errors.Wrap(err, "could not unmarshal queue request")
	}

	err = foo(request) // METHOD TO HANDLE REQUEST 
	if err != nil {

		ackErr := delivery.Ack(false)
		if err != nil {
			return errors.Wrap(ackErr, "could not ack")
		}
		return errors.Wrap(err, "could not run foo()")
	}

	err = delivery.Ack(false)
	if err != nil {
		return errors.Wrap(err, "could not ack")
	}
	return nil
}

deliveryConsumer := gorabbitmq.DeliveryConsumer(fn)

if service.queue == nil {
	log.Error(errors.Wrap(errors.New("could not consume closed queue"), ""))
	return
}

if service.queue.IsClosed() {
	log.Error(errors.Wrap(errors.New("queue channel was closed"), ""))
	return
}

err := service.queue.RegisterConsumer(consumerSettings, deliveryConsumer)
if err != nil {
	log.Error(errors.Wrap(err, "could not read items from the queue"))
}
```


## External packages

Go RabbitMQ Client Library https://github.com/streadway/amqp
