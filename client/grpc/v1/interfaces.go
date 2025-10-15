package v1

import pb "github.com/ValerySidorin/fujin/public/grpc/v1"

type Conn interface {
	Connect(id string) (Stream, error)
	Close() error
}

type Stream interface {
	Produce(topic string, p []byte) error
	Subscribe(topic string, autoCommit bool, handler func(msg *pb.FujinResponse_Message)) (uint32, error)
	Unsubscribe(subscriptionID uint32) error
	Close() error
}

type Subscription interface {
	Close() error
}
