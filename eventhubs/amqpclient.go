package eventhubs

import (
	"crypto/tls"
	"pack.ag/amqp"
)

type AmqpClient struct {
	*amqp.Client
}

func createTlsConnection(addr string) (*tls.Conn, error) {
	tlsConfig := &tls.Config{}
	tlsConn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return tlsConn, nil
}
