package rabbit

const (
	ExchangeName        = "delayed_exchange"
	ExchangeType        = "x-delayed-message"
	DelayedTypeHeader   = "x-delayed-type"
	DelayedTypeValue    = "direct"
	ContentTypeProtobuf = "application/protobuf"
)
