package client

import (
	"errors"
)

type Msg struct {
	Value []byte
	id    []byte
	r     *clientReader
}

func (m *Msg) Ack() error {
	if len(m.id) <= 0 {
		return nil
	}

	resp, err := m.r.ack(m.id)
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

	resp, err := m.r.nack(m.id)
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
