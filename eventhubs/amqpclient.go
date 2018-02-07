package eventhubs

import (
	"crypto/tls"
	"fmt"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type AmqpClient interface {
	CreateConnection(host string, user string, password string) error
	NewSender(target string) (electron.Sender, error)
	NewReceiver(source string, filters map[amqp.Symbol]interface{}) (electron.Receiver, error)
	Close(err error)
}

type ElectronClient struct {
	container electron.Container
	connection electron.Connection
	ackChannel chan electron.Outcome
}

const (
	PORT = "5671"
)

func (client *ElectronClient) CreateConnection(host string, user string, password string) error {

	container := electron.NewContainer(fmt.Sprintf("send[%v]", os.Getpid()))

	hostAndPort := fmt.Sprintf("%s:%s", host, PORT)
	tlsConn, err := createTlsConnection(hostAndPort)

	if err != nil {
		return err
	}

	connection, err := container.Connection(
		tlsConn, 
		electron.User(user), 
		electron.Password([]byte(password)), 
		electron.VirtualHost(host), 
		electron.SASLAllowedMechs("PLAIN"), 
		electron.SASLAllowInsecure(true),
		electron.Heartbeat(electron.Forever),
	)
	
	if err != nil {
		return err
	}

	client.container = container
	client.connection = connection

	return nil
}

func (client *ElectronClient) NewSender(target string) (electron.Sender, error) {

	sender, err := client.connection.Sender(electron.Target(target))

	return sender, err
}

func (client *ElectronClient) NewReceiver(source string, filters map[amqp.Symbol]interface{}) (electron.Receiver, error) {
	
	receiver, err := client.connection.Receiver(
		electron.Source(source),
		electron.Prefetch(true), 
		electron.Capacity(300),
		electron.Filter(filters),
	)

	return receiver, err
}

func (client *ElectronClient) Close (err error) {
	client.connection.Close(err)
}

func createTlsConnection(addr string) (*tls.Conn, error) {
	tlsConfig := &tls.Config{}
	tlsConn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return tlsConn, nil
}

/*
func (client *ElectronClient) SendAsync(outcomeChan chan electron.Outcome, eventData *EventData) {

	m := amqp.NewMessageWith(eventData.Data)
	client.sender.SendAsync(m, outcomeChan, body)	
}

func (client *ElectronClient) Receive() (*EventData, error) {

	msg, err := client.receiver.Receive()

	if err != nil {
		return nil, err
	}

	eventData, err := UnpackAmqpMessage(msg)

	return eventData, err
}
*/