package pubsub

import "context"

// Transport is the interface that publisher uses to send messages to the message broker agnostic of the underlying implementation.
type Transport interface {
	Send(ctx context.Context, event *Event) error
	HealthCheck() error
}

// TransportWrapper is a function that wraps a TransportFunc with additional functionality.
type TransportWrapper func(TransportFunc) TransportFunc

// TransportFunc is a function type that implements the Transport interface.
type TransportFunc func(ctx context.Context, event *Event) error

// HealthFunc calls the underlying transport's health check.
type HealthFunc func() error

// Send calls f(ctx, event).
func (f TransportFunc) Send(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

type transportWrapper struct {
	send   TransportFunc
	health HealthFunc
}

func (t transportWrapper) Send(ctx context.Context, event *Event) error {
	return t.send(ctx, event)
}

func (t transportWrapper) HealthCheck() error {
	return t.health()
}
