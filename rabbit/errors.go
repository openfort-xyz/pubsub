package rabbit

import "errors"

var (
	ErrConnClosed = errors.New("rabbitmq connection is closed")
	ErrChanClosed = errors.New("rabbitmq channel is closed")
	ErrChanNil = errors.New("rabbitmq channel is nil, please call Connect first")
	ErrHealthCheckFailed = errors.New("rabbitmq channel health check failed")
)