package middleware

import "github.com/Wr4thon/gorabbitmq/v2"

type errorCounter struct{}

const errorCounterKey = "@errorCounter"

// TODO validate
type ErrorCounterConfig struct {
	MaxRetries         int32
	MaxRetriesExceeded func(gorabbitmq.Context)
}

func ErrorCounterWithConfig(config ErrorCounterConfig) gorabbitmq.MiddlewareFunc {
	return func(hf gorabbitmq.HandlerFunc) gorabbitmq.HandlerFunc {
		return func(c gorabbitmq.Context) error {
			if v, ok := c.Delivery().Headers[errorCounterKey]; ok {
				if i, ok := v.(int32); ok {
					c.Set(errorCounter{}, i)
				}
			} else {
				c.Set(errorCounter{}, int32(0))
			}

			err := hf(c)

			if err != nil {
				c.Ack(false)
				table := c.Delivery().Headers
				if table == nil {
					table = make(map[string]interface{})
				}

				if errorCounter, ok := c.Value(errorCounter{}).(int32); ok {
					if errorCounter >= config.MaxRetries {
						config.MaxRetriesExceeded(c)
						c.Nack(false, false)
						return nil
					}

					table[errorCounterKey] = errorCounter + 1
				}

				c.Queue().SendWithTable(c.DeliveryContext(), c.Delivery().Body, table)
			}

			return err
		}
	}
}
