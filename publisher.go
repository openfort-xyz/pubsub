package pubsub

import (
	"context"
)

// Publisher is the struct that sends messages to the message broker using the Transport interface agnostic of the underlying implementation.
type Publisher struct {
	Transport Transport
}

// NewPublisher creates a new publisher with the given transport.
func NewPublisher(transport Transport) *Publisher {
	return &Publisher{Transport: transport}
}

// Use adds a transport wrapper to the publisher.
func (p *Publisher) Use(transport TransportWrapper) {
	p.Transport = transport(p.Transport)
}

// Publish sends the event to the message broker.
func (p *Publisher) Publish(ctx context.Context, event *Event) error {
	return p.Transport.Send(ctx, event)
}
