package eventhubs

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"pack.ag/amqp"
	"time"
)

// EventHub Connection Strings
const (
	EventHubHost         = "amqps://%s.servicebus.windows.net"
	EventHubReceiverPath = "%s/ConsumerGroups/%s/Partitions/%d"
)

// EventHub Main Client
type EventHubClient struct {
	amqpClient *amqp.Client
	context    *context.Context
	session    *amqp.Session
	sender     *amqp.Sender
	receiver   *amqp.Receiver
	Options    ConnectionOptions
	Logger     io.Writer
}

// Options for Connecting to Event Hub
type ConnectionOptions struct {
	EventHubNamespace     string
	EventHubName          string
	EventHubAccessKeyName string
	EventHubAccessKey     string
}

// Create Event Hub Client with Options
func New(options *ConnectionOptions) (client *EventHubClient) {
	client = &EventHubClient{
		Options: *options,
		Logger:  os.Stdout,
	}
	return client
}

// Create Connection to Event Hub
func (client *EventHubClient) CreateConnection() error {

	host := fmt.Sprintf(EventHubHost, client.Options.EventHubNamespace)
	accessKeyName := client.Options.EventHubAccessKeyName
	accessKey := client.Options.EventHubAccessKey

	amqpClient, err := amqp.Dial(
		host,
		amqp.ConnSASLPlain(accessKeyName, accessKey),
	)

	session, err := amqpClient.NewSession()
	if err != nil {
		return err
	}

	ctx := context.Background()

	client.amqpClient = amqpClient
	client.session = session
	client.context = &ctx

	return err
}

// Create Event Hub Sender
func (client *EventHubClient) CreateSender() error {

	sender, err := client.session.NewSender(
		amqp.LinkAddress(client.Options.EventHubName),
	)
	if err != nil {
		return err
	}

	client.sender = sender

	return nil
}

// Send a message to Event Hub
func (client *EventHubClient) Send(message string) error {

	ctx, cancel := context.WithTimeout(*client.context, 5*time.Second)
	defer cancel()

	// Send message
	err := client.sender.Send(ctx, &amqp.Message{
		Data: []byte(message),
	})
	if err != nil {
		return err
	}

	fmt.Println("Successfully sent!")

	return nil
}

// Create Event Hub Receiver
func (client *EventHubClient) CreateReceiver(consumerGroup string, partition int) error {

	receiverPath := fmt.Sprintf(
		EventHubReceiverPath,
		client.Options.EventHubName,
		consumerGroup,
		partition,
	)

	// Create a receiver
	receiver, err := client.session.NewReceiver(
		amqp.LinkAddress(receiverPath),
		amqp.LinkCredit(10),
		amqp.LinkBatching(true),
	)

	if err != nil {
		return err
	}

	client.receiver = receiver

	return nil

}

func (client *EventHubClient) Receive() error {
	ctx, cancel := context.WithCancel(*client.context)
	defer cancel()

	for {
		msg, err := client.receiver.Receive(ctx)
		if err != nil {
			return err
		}
		msg.Accept()
		fmt.Printf("Message received: %s\n", msg.Data)
	}
}

// Close Connection to Event Hub
func (client *EventHubClient) Close() error {

	if client.sender != nil {
		err := client.sender.Close()
		if err != nil {
			return err
		}
	}

	if client.receiver != nil {
		err := client.receiver.Close()
		if err != nil {
			return err
		}
	}

	err := client.Close()
	if err != nil {
		return err
	}
	return nil
}

func createTlsConnection(addr string) (*tls.Conn, error) {
	tlsConfig := &tls.Config{}
	tlsConn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return tlsConn, nil
}
