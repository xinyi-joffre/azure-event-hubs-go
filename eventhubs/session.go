package eventhubs

import (
	"github.com/satori/go.uuid"
	"pack.ag/amqp"
)

type Session struct {
	*amqp.Session
	SessionID string
}

func NewSession(amqpSession *amqp.Session) (*Session, error) {
	id, err := uuid.NewV4()

	if err != nil {
		return nil, err
	}

	return &Session{
		Session:   amqpSession,
		SessionID: id.String(),
	}, nil
}
