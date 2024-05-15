package rabbit

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
	"go.openfort.xyz/pubsub"
)

type rabbitListener struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	amqpURL    string
	logger     *slog.Logger
}

func NewRabbitListener(amqpURL string, logger *slog.Logger) pubsub.Listener {
	return &rabbitListener{
		amqpURL: amqpURL,
		logger:  logger,
	}
}

func (r *rabbitListener) Connect(ctx context.Context) error {
	if r.logger != nil {
		r.logger.InfoContext(ctx, "Connecting to RabbitMQ")
	}
	conn, err := amqp091.Dial(r.amqpURL)
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Connecting to RabbitMQ", slog.String("error", err.Error()))
		}
		return err
	}

	if r.logger != nil {
		r.logger.InfoContext(ctx, "Connecting to channel")
	}
	ch, err := conn.Channel()
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Connecting to channel", slog.String("error", err.Error()))
		}
		_ = conn.Close()
		return err
	}

	if r.logger != nil {
		r.logger.InfoContext(ctx, "Declaring exchange")
	}
	err = ch.ExchangeDeclare(ExchangeName, ExchangeType, true, false, false, false, amqp091.Table{DelayedTypeHeader: DelayedTypeValue})
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Declaring exchange", slog.String("error", err.Error()))
		}
		return err
	}

	r.connection = conn
	r.channel = ch
	return nil
}

func (r *rabbitListener) ensureTopic(ctx context.Context, topic pubsub.Topic) error {
	if r.logger != nil {
		r.logger.InfoContext(ctx, "Ensuring topic", slog.String("topic", topic.String()))
	}
	_, err := r.channel.QueueDeclare(topic.String(), true, false, false, false, nil)
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Ensuring topic", slog.String("topic", topic.String()), slog.String("error", err.Error()))
		}
		return err
	}

	if r.logger != nil {
		r.logger.InfoContext(ctx, "Binding topic", slog.String("topic", topic.String()))
	}
	err = r.channel.QueueBind(topic.String(), topic.String(), ExchangeName, false, nil)
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Binding topic", slog.String("topic", topic.String()), slog.String("error", err.Error()))
		}
		return err
	}

	return nil
}

func (r *rabbitListener) Subscribe(ctx context.Context, subscription *pubsub.Subscription) error {
	if r.logger != nil {
		r.logger.InfoContext(ctx, "Subscribing to topic", slog.String("topic", subscription.Topic.String()))
	}
	err := r.ensureTopic(ctx, subscription.Topic)
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Subscribing to topic", slog.String("topic", subscription.Topic.String()), slog.String("error", err.Error()))
		}
		return err
	}

	if r.logger != nil {
		r.logger.InfoContext(ctx, "Consuming messages", slog.String("topic", subscription.Topic.String()))
	}
	messages, err := r.channel.Consume(subscription.Topic.String(), subscription.Consumer, false, false, false, false, nil)
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Consuming messages", slog.String("topic", subscription.Topic.String()), slog.String("error", err.Error()))
		}
		return err
	}

	for {
		select {
		case <-ctx.Done():
			if r.logger != nil {
				r.logger.InfoContext(ctx, "Context done")
			}
			return nil
		case msg, ok := <-messages:
			if !ok {
				if r.logger != nil {
					r.logger.InfoContext(ctx, "Channel closed")
				}
				_ = msg.Nack(false, true)
				return errors.New("channel closed")
			}
			if r.logger != nil {
				r.logger.InfoContext(ctx, "Message received", slog.String("topic", subscription.Topic.String()))
			}
			event := pubsub.NewEvent(subscription.Topic, msg.Body)
			event.Metadata = pubsub.Metadata(msg.Headers)
			if r.logger != nil {
				r.logger.InfoContext(ctx, "Sending event to subscription channel", slog.String("topic", subscription.Topic.String()))
			}
			subscription.Channel <- event
			if r.logger != nil {
				r.logger.InfoContext(ctx, "Event sent to subscription channel, sending ack", slog.String("topic", subscription.Topic.String()))
			}
			_ = msg.Ack(false)
		}
	}
}

func (r *rabbitListener) Close() error {
	errCh := r.channel.Close()
	errCo := r.connection.Close()
	return errors.Join(errCh, errCo)
}
