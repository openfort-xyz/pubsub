package pubsub

import "context"

// Transport is the interface that publisher uses to send messages to the message broker agnostic of the underlying implementation.
type Transport interface {
	Send(ctx context.Context, event *Event) error
	HealthCheck() error
}

// TransportWrapper is a function that wraps a transport with additional functionality.
type TransportWrapper func(Transport) Transport

// TransportFunc is a function type that implements the Transport interface.
type TransportFunc func(ctx context.Context, event *Event) error

// Send calls f(ctx, event).
func (f TransportFunc) Send(ctx context.Context, event *Event) error {
	return f(ctx, event)
}
