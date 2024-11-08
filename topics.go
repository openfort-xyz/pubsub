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

	// TransactionSentTopic is the topic for transaction sent.
	TransactionSentTopic Topic = "transaction.sent"

	// TransactionIndexedTopic is the topic for transaction indexed.
	TransactionIndexedTopic Topic = "transaction.indexed"

	// TransactionDroppedTopic is the topic for transaction dropped.
	TransactionDroppedTopic Topic = "transaction.dropped"

	// TransactionCompletedTopic is the topic for transaction completed.
	TransactionCompletedTopic Topic = "transaction.completed"
)
