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