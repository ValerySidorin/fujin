package fujin

import (
	"errors"
)

type Msg struct {
	Value   []byte
	Headers map[string]string
	id      []byte
	subID   byte
	s       *Stream
}

func (m *Msg) Ack() error {
	if len(m.id) <= 0 {
		return nil
	}

	resp, err := m.s.ack(m.subID, m.id)
	if err != nil {
		return err
	}

	if len(resp.AckMsgResponses) < 1 {
		return errors.New("invalid response")
	}

	return resp.AckMsgResponses[0].Err
}

func (m *Msg) Nack() error {
	if len(m.id) <= 0 {
		return nil
	}

	resp, err := m.s.nack(m.subID, m.id)
	if err != nil {
		return err
	}

	if len(resp.AckMsgResponses) < 1 {
		return errors.New("invalid response")
	}

	return resp.AckMsgResponses[0].Err
}

func (m Msg) String() string {
	return string(m.Value)
}
