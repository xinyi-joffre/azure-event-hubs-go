package eventhubs

import (
	"fmt"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"sync"
)

const (
	EventHubPartitionedSenderTarget = "%s/Partitions/%d" // e.g. eventhubname/Partitions/0
)

type Sender struct {
	client  AmqpClient
	sender  electron.Sender
	target  string
	senderMutex	sync.Mutex
}

// SendOption provides a way to customize a message on sending
type SendOption func(eventData *EventData) error

func NewSender(client AmqpClient, eventHubName string) (*Sender, error) {

	sender := &Sender{
		client: client,
		target: eventHubName,
	}

	err := sender.newSessionAndLink()
	if err != nil {
		return nil, err
	}

	return sender, nil
}

func (sender *Sender) newSessionAndLink() error {
	
		amqpSender, err := sender.client.NewSender(sender.target)
		if err != nil {
			return err
		}
	
		sender.sender = amqpSender
		return nil
}

func (sender *Sender) Send(eventData *EventData, opts ...SendOption) error {

	// TODO: Add in recovery logic in case the link / session has gone down

	for _, opt := range opts {
		opt(eventData)
	}

	annotations := make(map[amqp.AnnotationKey]interface{})

	if eventData.PartitionKey != "" {
		annotations[amqp.AnnotationKeyString("x-opt-partition-key")] = eventData.PartitionKey
	}

	m := amqp.NewMessage()
	m.SetInferred(true)
	m.Marshal([]byte(eventData.Data))

	m.SetMessageAnnotations(annotations)
	out := <-sender.sender.SendWaitable(m)

	if out.Error != nil {
		fmt.Printf("Err: send err: %v", out.Error)
		panic(out.Error)
		return out.Error
	}
	fmt.Printf("acknowledgement[%v] %v error: %v", out.Status, out.Value, out.Error)	

	return nil
}

// Close sender connections and session
func (sender *Sender) Close(err error) {
	
		sender.sender.Close(err)
		
		return
}

/*
type Sender struct {
	client  *AmqpClient
	session *Session
	sender  *amqp.Sender
	target  string
	senderMutex sync.Mutex
}

func NewSender(client *AmqpClient, eventHubName string) (*Sender, error) {

	sender := &Sender{
		client: client,
		target: eventHubName,
	}

	err := sender.newSessionAndLink()
	if err != nil {
		return nil, err
	}

	return sender, nil
}

func NewPartitionedSender(client *AmqpClient, eventHubName string, partition int) (*Sender, error) {

	var partitionedTarget = fmt.Sprintf(EventHubPartitionedSenderTarget, eventHubName, partition)

	sender := &Sender{
		client: client,
		target: partitionedTarget,
	}

	err := sender.newSessionAndLink()
	if err != nil {
		return nil, err
	}

	return sender, nil
}

func (sender *Sender) newSessionAndLink() error {

	amqpSession, err := sender.client.NewSession()
	if err != nil {
		return err
	}

	amqpSender, err := amqpSession.NewSender(amqp.LinkAddress(sender.target))
	if err != nil {
		return err
	}

	sender.session, err = NewSession(amqpSession)
	if err != nil {
		return err
	}

	sender.sender = amqpSender
	return nil
}

func (sender *Sender) Send(ctx context.Context, eventData *EventData, opts ...SendOption) error {
	// TODO: Add in recovery logic in case the link / session has gone down

	for _, opt := range opts {
		opt(eventData)
	}

	msg := &amqp.Message{
		Data: []byte(eventData.Data),
		Annotations: make(map[interface{}]interface{}),
	}

	if eventData.PartitionKey != "" {
		msg.Annotations["x-opt-partition-key"] = eventData.PartitionKey
	}

	err := sender.sender.Send(ctx, msg)
	if err != nil {
		fmt.Printf("Err: send err: %v", err)
		return err
	}

	return nil
}

// Recover will attempt to close the current session and link, then rebuild them
func (s *Sender) Recover() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = s.newSessionAndLink()
	if err != nil {
		return err
	}

	return nil
}

// Close sender connections and session
func (sender *Sender) Close() error {

	err := sender.sender.Close()
	if err != nil {
		return err
	}

	err = sender.session.Close()
	if err != nil {
		return err
	}

	return nil
}

// SendOption provides a way to customize a message on sending
type SendOption func(eventData *EventData) error

func WithPartitionKey(partitionKey string) SendOption {
	return func(eventData *EventData) error {
		eventData.PartitionKey = partitionKey
		return nil
	}
}
*/