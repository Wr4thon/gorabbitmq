package gorabbitmq

// ConnectionSettings holds settings for a rabbitMQConnector connection
type ConnectionSettings struct {
	UserName string
	Password string
	Host     string
	Port     int
}

// QueueSettings holds the configurationf for a Channel
type QueueSettings struct {
	QueueName        string
	Durable          bool
	Exclusive        bool
	DeleteWhenUnused bool
	NoWait           bool
}
