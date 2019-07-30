package gorabbitmq

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

// QueueSettings holds the configurationf for a Channel
type QueueSettings struct {
	// QueueName containes the name of the queue
	QueueName string `json:"queueName,omitempty"`
	// Durable determines, whether or not the data contained in the queue will be persisted by the server on shutdown
	Durable bool `json:"durable,omitempty"`
	// Exclusive means used by only one connection and the queue will be deleted when that connection closes
	Exclusive bool `json:"exclusive,omitempty"`
	// DeleteWhenUnused means the queue is deleted when the last consumer unsubscribes
	DeleteWhenUnused bool `json:"deleteWhenUnused,omitempty"`
	// NoWait ???
	NoWait bool `json:"noWait,omitempty"`
}
