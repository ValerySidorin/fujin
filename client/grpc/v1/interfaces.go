package v1

import "github.com/ValerySidorin/fujin/client/models"

type Conn interface {
	Connect(id string) (Stream, error)
	Close() error
}

type Stream interface {
	Produce(topic string, p []byte) error
	Subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error)
	Unsubscribe(subscriptionID uint32) error
	Ack(subscriptionID uint32, messageIDs ...[]byte) (models.AckResult, error)
	Nack(subscriptionID uint32, messageIDs ...[]byte) (models.NackResult, error)
	Close() error
}

type Subscription interface {
	Close() error
}
