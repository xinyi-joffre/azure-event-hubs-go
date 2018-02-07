package eventhubs

import (
	"pack.ag/amqp"
)

type Session struct {
	*amqp.Session
	SessionID string
}

func NewSession(amqpSession *amqp.Session) (*Session, error) {

	return &Session{
		Session:   amqpSession,
	}, nil
}
