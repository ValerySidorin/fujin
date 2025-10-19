package models

type Msg struct {
	SubscriptionID uint32
	MessageID      []byte
	Payload        []byte
	Headers        map[string]string
}

// AckResult contains the result of an ack operation
type AckResult struct {
	Error          error              // General error
	MessageResults []AckMessageResult // Per-message results
}

// AckMessageResult contains the result for a specific message
type AckMessageResult struct {
	MessageID []byte
	Error     error
}

// NackResult contains the result of a nack operation
type NackResult struct {
	Error          error               // General error
	MessageResults []NackMessageResult // Per-message results
}

// NackMessageResult contains the result for a specific message
type NackMessageResult struct {
	MessageID []byte
	Error     error
}

// FetchResult contains the result of a fetch operation
type FetchResult struct {
	Error          error  // General error
	SubscriptionID uint32 // Subscription ID for ack/nack
	Messages       []Msg  // Fetched messages
}
