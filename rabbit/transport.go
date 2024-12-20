package rabbit

import (
	"context"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
	"go.openfort.xyz/pubsub"
)

type rabbitTransport struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	durable    bool
	exchange   string
}

var _ pubsub.Transport = (*rabbitTransport)(nil)

type TransportOption func(*rabbitTransport)

func WithDurable(durability bool) TransportOption {
	return func(r *rabbitTransport) {
		r.durable = durability
	}
}

func WithDelayed() TransportOption {
	return func(r *rabbitTransport) {
		r.durable = true
		r.exchange = ExchangeNameDelayed
	}
}

func NewRabbitTransport(amqpURL string, opts ...TransportOption) (pubsub.Transport, error) {
	conn, err := amqp091.Dial(amqpURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	tp := &rabbitTransport{
		connection: conn,
		channel:    ch,
		durable:    true,
		exchange:   ExchangeName,
	}

	for _, opt := range opts {
		opt(tp)
	}

	return tp, nil
}

func (r *rabbitTransport) Send(ctx context.Context, event *pubsub.Event) error {
	ch, err := r.connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = r.channel.QueueDeclare(event.Topic.String(), r.durable, false, false, false, nil)
	if err != nil {
		return err
	}

	headers := make(amqp091.Table)
	for k, v := range event.Metadata {
		headers[k] = v
	}

	return r.channel.PublishWithContext(ctx,
		r.exchange,
		event.Topic.String(),
		false,
		false,
		amqp091.Publishing{
			Headers:     headers,
			ContentType: ContentTypeProtobuf,
			Body:        event.Payload,
		},
	)
}

func (r *rabbitTransport) HealthCheck() error {
	if r.connection == nil || r.connection.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}

	if r.channel == nil || r.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}

	if err := r.channel.Confirm(false); err != nil {
		return fmt.Errorf("rabbitmq health check failed: %w", err)
	}

	return nil
}
