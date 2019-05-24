package gorabbitmq

// ConnectionSettings holds settings for a rabbitMQConnector connection
type ConnectionSettings struct {
	UserName string `json:user_name`
	Password string `json:password`
	Host     string `json:host`
	Port     int    `json:port`
}

// QueueSettings holds the configurationf for a Channel
type QueueSettings struct {
	QueueName        string `json:queue_name`
	Durable          bool   `json:durable`
	Exclusive        bool   `json:exclusive`
	DeleteWhenUnused bool   `json:delete_when_unused`
	NoWait           bool   `json:no_wait`
}
