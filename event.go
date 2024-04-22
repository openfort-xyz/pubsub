package pubsub

// Event represents a message published to a topic.
type Event struct {
	Topic    Topic    `json:"event"`
	Metadata Metadata `json:"metadata,omitempty"`
	Payload  []byte   `json:"payload"`
}

// NewEvent creates a new event.
func NewEvent(topic Topic, payload []byte) *Event {
	return &Event{
		Topic:   topic,
		Payload: payload,
	}
}
