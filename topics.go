package pubsub

// Topic represents a message topic.
type Topic string

// String returns the string representation of the topic.
func (t Topic) String() string {
	return string(t)
}

const (
	// SendNotificationTopic is the topic for sending notifications.
	SendNotificationTopic Topic = "send.notification"

	// UserOperationTopic is the topic for user operations.
	UserOperationTopic Topic = "user.operation"

	// TransactionUpdatedTopic is the topic for transaction updates.
	TransactionUpdatedTopic Topic = "transaction.updated"

	// WriteMetricTopic is the topic for writing metrics.
	WriteMetricTopic Topic = "write.metric"
)
