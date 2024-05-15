package pubsub

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// Subscriber is the struct that receives events from the listener and calls the appropriate Handler using the Listener interface agnostic of the underlying implementation.
type Subscriber struct {
	eventHandlers map[Topic]Handler
	middlewares   []Middleware
	listener      Listener
	running       *atomic.Bool
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	subscriptions []*Subscription
	cancel        context.CancelFunc
}

// NewSubscriber creates a new Subscriber with the given Listener.
func NewSubscriber(listener Listener) *Subscriber {
	return &Subscriber{
		eventHandlers: make(map[Topic]Handler),
		middlewares:   make([]Middleware, 0),
		listener:      listener,
		subscriptions: make([]*Subscription, 0),
		running:       new(atomic.Bool),
	}
}

// HandleFunc adds a Handler to the Subscriber for the given Topic.
func (s *Subscriber) HandleFunc(topic Topic, handler Handler) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.eventHandlers[topic] = handler
}

// Use adds a Middleware to the Subscriber.
func (s *Subscriber) Use(middleware Middleware) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.middlewares = append(s.middlewares, middleware)
}

func (s *Subscriber) handle(ctx context.Context, event *Event) error {
	s.mutex.RLock()
	handler, ok := s.eventHandlers[event.Topic]
	s.mutex.RUnlock()
	if !ok {
		return nil
	}

	return handler(ctx, event)
}

func (s *Subscriber) subscriber(ctx context.Context, subscription *Subscription) {
	defer s.wg.Done()
	for s.running.Load() {
		go func() {
			err := s.listener.Subscribe(ctx, subscription)
			if err != nil {
				close(subscription.Channel)
				return
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-subscription.Channel:
				if !ok {
					return
				}
				_ = s.handle(ctx, ev)
			}
		}
	}
}

// Start starts the Subscriber and listens for events from the Listener.
func (s *Subscriber) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	err := s.listener.Connect(ctx)
	if err != nil {
		return err
	}

	s.mutex.Lock()
	for topic, handler := range s.eventHandlers {
		for _, middleware := range s.middlewares {
			handler = middleware(handler)
		}
		s.eventHandlers[topic] = handler
		s.subscriptions = append(s.subscriptions, NewSubscription(topic))
	}
	s.mutex.Unlock()

	s.running.Store(true)

	for _, sub := range s.subscriptions {
		s.wg.Add(1)
		go s.subscriber(ctx, sub)
	}

	s.wg.Wait()
	return nil
}

// Stop stops gracefully the Subscriber and closes the Listener.
func (s *Subscriber) Stop(_ context.Context) error {
	s.running.Store(false)
	s.cancel()
	return s.listener.Close()
}

// Handler is the function that processes the event.
type Handler func(ctx context.Context, event *Event) error

// Middleware is the function that wraps a Handler to add functionality.
type Middleware func(next Handler) Handler

// Subscription is the struct that represents a subscription to a Topic with a channel to receive Event.
type Subscription struct {
	Topic    Topic
	Consumer string
	Channel  chan *Event
}

// NewSubscription creates a new Subscription for the given Topic.
func NewSubscription(topic Topic) *Subscription {
	return &Subscription{
		Topic:    topic,
		Consumer: uuid.NewString(),
		Channel:  make(chan *Event),
	}
}
