package client

type Msg struct {
	Value []byte
	meta  []byte
	r     *clientReader
}

func (m *Msg) Ack() error {
	return m.r.ack(m.meta)
}

func (m *Msg) NAck() error {
	return m.r.nAck(m.meta)
}

func (m Msg) String() string {
	return string(m.Value)
}
