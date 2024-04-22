package rabbit

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
	"go.openfort.xyz/pubsub"
)

type rabbitTransport struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
}

var _ pubsub.Transport = (*rabbitTransport)(nil)

func NewRabbitTransport(amqpURL string) (pubsub.Transport, error) {
	conn, err := amqp091.Dial(amqpURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &rabbitTransport{
		connection: conn,
		channel:    ch,
	}, nil
}

func (r *rabbitTransport) Send(ctx context.Context, event *pubsub.Event) error {
	_, err := r.channel.QueueDeclare(event.Topic.String(), true, false, false, false, nil)
	if err != nil {
		return err
	}

	headers := make(amqp091.Table)
	for k, v := range event.Metadata {
		headers[k] = v
	}

	return r.channel.PublishWithContext(ctx,
		ExchangeName,
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
