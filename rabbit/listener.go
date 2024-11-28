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
	delayed    bool
	durable    bool
}

type ListenerOption func(*rabbitListener)

func WithLogger(logger *slog.Logger) ListenerOption {
	return func(r *rabbitListener) {
		r.logger = logger
	}
}

func WithDurability(durability bool) ListenerOption {
	return func(r *rabbitListener) {
		r.durable = durability
	}
}

func WithDelayedDial() ListenerOption {
	return func(r *rabbitListener) {
		r.delayed = true
		r.durable = true
	}
}

func NewRabbitListener(amqpURL string, opts ...ListenerOption) pubsub.Listener {
	l := &rabbitListener{
		amqpURL: amqpURL,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
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

	var exchangeName, exchangeType string
	var table amqp091.Table
	if r.delayed {
		exchangeName = ExchangeNameDelayed
		exchangeType = ExchangeTypeDelayed
		table = amqp091.Table{DelayedTypeHeader: DelayedTypeValue}
	} else {
		exchangeName = ExchangeName
		exchangeType = ExchangeType
	}
	err = ch.ExchangeDeclare(exchangeName, exchangeType, r.durable, false, false, false, table)
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
	_, err := r.channel.QueueDeclare(topic.String(), r.delayed, false, false, false, nil)
	if err != nil {
		if r.logger != nil {
			r.logger.ErrorContext(ctx, "Ensuring topic", slog.String("topic", topic.String()), slog.String("error", err.Error()))
		}
		return err
	}

	if r.logger != nil {
		r.logger.InfoContext(ctx, "Binding topic", slog.String("topic", topic.String()))
	}

	var exchangeName string
	if r.delayed {
		exchangeName = ExchangeNameDelayed
	} else {
		exchangeName = ExchangeName
	}
	err = r.channel.QueueBind(topic.String(), topic.String(), exchangeName, false, nil)
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
					r.logger.ErrorContext(ctx, "Channel closed")
				}
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
	var errCh, errCo error
	if r.channel != nil {
		errCh = r.channel.Close()
	}
	if r.connection != nil {
		errCo = r.connection.Close()
	}
	return errors.Join(errCh, errCo)
}
