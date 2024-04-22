package rabbit

import (
	"context"
	"errors"
	"github.com/rabbitmq/amqp091-go"
	"go.openfort.xyz/pubsub"
)

type rabbitListener struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	amqpURL    string
}

func NewRabbitListener(amqpURL string) pubsub.Listener {
	return &rabbitListener{
		amqpURL: amqpURL,
	}
}

func (r *rabbitListener) Connect(ctx context.Context) error {
	conn, err := amqp091.Dial(r.amqpURL)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}

	err = ch.ExchangeDeclare(ExchangeName, ExchangeType, true, false, false, false, amqp091.Table{DelayedTypeHeader: DelayedTypeValue})
	if err != nil {
		return err
	}

	r.connection = conn
	r.channel = ch
	return nil
}

func (r *rabbitListener) ensureTopic(topic pubsub.Topic) error {
	_, err := r.channel.QueueDeclare(topic.String(), true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = r.channel.QueueBind(topic.String(), topic.String(), ExchangeName, false, nil)
	if err != nil {
		return err
	}

	return nil
}

func (r *rabbitListener) Subscribe(ctx context.Context, subscription *pubsub.Subscription) error {
	err := r.ensureTopic(subscription.Topic)
	messages, err := r.channel.Consume(subscription.Topic.String(), subscription.Consumer, false, false, false, false, nil)
	if err != nil {
		return err
	}

	for msg := range messages {
		event := pubsub.NewEvent(subscription.Topic, msg.Body)
		event.Metadata = pubsub.Metadata(msg.Headers)
		subscription.Channel <- event
		_ = msg.Ack(false)
	}

	return nil
}

func (r *rabbitListener) Close() error {
	errCh := r.channel.Close()
	errCo := r.connection.Close()
	return errors.Join(errCh, errCo)
}
