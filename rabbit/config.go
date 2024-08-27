package rabbit

const (
	ExchangeName        = "exchange"
	ExchangeType        = "direct"
	ExchangeNameDelayed = "delayed_exchange"
	ExchangeTypeDelayed = "x-delayed-message"
	DelayedTypeHeader   = "x-delayed-type"
	DelayedTypeValue    = "direct"
	ContentTypeProtobuf = "application/protobuf"
)
