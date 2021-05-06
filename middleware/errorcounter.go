package middleware

import (
	"github.com/pkg/errors"

	"github.com/Wr4thon/gorabbitmq/v3"
)

// ErrorCounter can be used to extract the ErrorCounter from the context.
type ErrorCounter struct{}

const errorCounterKey = "@errorCounter"

// ErrorCounterConfig configures the ErrorCounter Middleware.
// TODO validate
type ErrorCounterConfig struct {
	MaxRetries         int32
	MaxRetriesExceeded func(gorabbitmq.Context, error) error
}

// ErrorCounterWithConfig is a middleware that counts errors when they occur.
// When they exceed a configurable Threshold a custom callback gets called.
func ErrorCounterWithConfig(config ErrorCounterConfig) gorabbitmq.MiddlewareFunc {
	return func(hf gorabbitmq.HandlerFunc) gorabbitmq.HandlerFunc {
		return func(c gorabbitmq.Context) error {
			if v, ok := c.Delivery().Headers[errorCounterKey]; ok {
				if i, ok := v.(int32); ok {
					c.Set(ErrorCounter{}, i)
				}
			} else {
				c.Set(ErrorCounter{}, int32(0))
			}

			err := hf(c)

			if err != nil {
				table := c.Delivery().Headers
				if table == nil {
					table = make(map[string]interface{})
				}

				if errorCounter, ok := c.Value(ErrorCounter{}).(int32); ok {
					if errorCounter >= config.MaxRetries {
						retryError := config.MaxRetriesExceeded(c, err)
						if retryError != nil {
							return errors.Wrapf(retryError, "failed to execute maxRetriesExceeded handler, after %v", err)
						}

						c.Nack(false, false)
						return nil
					}

					table[errorCounterKey] = errorCounter + 1
				}

				requeueErr := c.Queue().SendWithTable(c.DeliveryContext(), c.Delivery().Body, table)
				if requeueErr != nil {
					return errors.Wrapf(requeueErr, "failed to requeue message, after %v", err)
				}
			}

			c.Ack(false)
			return err
		}
	}
}
