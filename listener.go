package pubsub

import "context"

// Listener is the interface that subscriber uses to connect to the message broker agnostic of the underlying implementation.
type Listener interface {
	// Connect connects to the message broker.
	Connect(ctx context.Context) error

	// Subscribe subscribes to a topic and returns messages to the subscription channel.
	Subscribe(ctx context.Context, subscription *Subscription) error

	// Close closes the connection to the message broker.
	Close() error
}
