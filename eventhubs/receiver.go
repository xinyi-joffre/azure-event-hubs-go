package eventhubs

import (
	"context"
	"fmt"
	"net"
	"pack.ag/amqp"
	"time"
)

const (
	EventHubReceiverPath = "%s/ConsumerGroups/%s/Partitions/%d" // i.e. eventhubname/ConsumerGroups/consumergroupname/Partitions/partition
)

type Receiver struct {
	client        *AmqpClient
	session       *Session
	receiver      *amqp.Receiver
	consumerGroup string
	partition     int
	target        string
	filters       []amqp.LinkOption
	done          chan struct{}
}

type ReceiveOption func(receiver *Receiver) error

func NewReceiver(client *AmqpClient, eventHubName string, consumerGroup string, partition int, opts ...ReceiveOption) (*Receiver, error) {
	targetPath := fmt.Sprintf(
		EventHubReceiverPath,
		eventHubName,
		consumerGroup,
		partition,
	)

	receiver := &Receiver{
		client:        client,
		consumerGroup: consumerGroup,
		partition:     partition,
		target:        targetPath,
		done:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(receiver)
	}

	err := receiver.newSessionAndLink()
	if err != nil {
		return nil, err
	}

	return receiver, nil
}

func (receiver *Receiver) newSessionAndLink() error {

	amqpSession, err := receiver.client.NewSession()
	if err != nil {
		return err
	}

	linkOptions := append(
		receiver.filters,
		amqp.LinkAddress(receiver.target),
		amqp.LinkCredit(10),
		amqp.LinkBatching(true),
	)

	amqpReceiver, err := amqpSession.NewReceiver(
		linkOptions...,
	)
	if err != nil {
		return err
	}

	receiver.receiver = amqpReceiver

	receiver.session, err = NewSession(amqpSession)
	if err != nil {
		return err
	}

	return nil
}

func WithOffsetFilter(offset string) ReceiveOption {
	return func(receiver *Receiver) error {
		offsetFilter := fmt.Sprintf("amqp.annotation.x-opt-offset > '%s'", offset)
		receiver.filters = append(receiver.filters, amqp.LinkSelectorFilter(offsetFilter))
		return nil
	}
}

func WithTimeEnqueuedFilter(timeEnqueued time.Time) ReceiveOption {
	return func(receiver *Receiver) error {
		timeEnqueuedFilter := fmt.Sprintf("amqp.annotation.x-opt-enqueued-time > %d", (timeEnqueued.UnixNano() / 1000000))
		receiver.filters = append(receiver.filters, amqp.LinkSelectorFilter(timeEnqueuedFilter))
		return nil
	}
}

func (receiver *Receiver) Close() error {
	
	close(receiver.done)

	err := receiver.receiver.Close()
	if err != nil {
		return err
	}

	err = receiver.session.Close()
	if err != nil {
		return err
	}

	return nil
}

func (receiver *Receiver) Recover() error {
	err := receiver.Close()
	if err != nil {
		return nil
	}

	err = receiver.newSessionAndLink()
	if err != nil {
		return err
	}

	return nil
}

// Receive one message
func (receiver *Receiver) Receive() (*EventData, error) {
	
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
	
		for {

			msg, err := receiver.receiver.Receive(ctx)

			if err != nil {
				msg.Reject()
				fmt.Println("Message rejected.")				
				return nil, err
			} else {
				msg.Accept()
				fmt.Println("Message accepted.")				
			}

			eventData, err := UnpackAmqpMessage(msg)				
			return eventData, err
		}
	}
	

func (receiver *Receiver) Listen(handler Handler) {
	messages := make(chan *amqp.Message)
	go receiver.listenForMessages(messages)
	go receiver.handleMessages(messages, handler)
}

func (receiver *Receiver) handleMessages(messages chan *amqp.Message, handler Handler) {
	for {
		select {
		case <-receiver.done:
			fmt.Printf("Done handling messages.\n")
			return
		case msg := <-messages:
			ctx := context.Background()
			var err error

			if err == nil && msg != nil {
				eventData, _ := UnpackAmqpMessage(msg)	

				err = handler(ctx, eventData)
			}

			if err != nil {
				msg.Reject()
				fmt.Println("Message rejected.")
			} else {
				msg.Accept()
				fmt.Println("Message accepted.")
			}
		}
	}
}

func (receiver *Receiver) listenForMessages(msgChan chan *amqp.Message) {
	for {
		select {
		case <-receiver.done:
			fmt.Printf("done listening for messages")
			close(msgChan)
			return
		default:
			waitCtx, cancel := context.WithTimeout(context.Background(), Forever)
			msg, err := receiver.receiver.Receive(waitCtx)
			cancel()

			// TODO: handle receive errors better. It's not sufficient to check only for timeout
			if err, ok := err.(net.Error); ok && err.Timeout() {
				fmt.Printf("Attempting to receive messages timed out.\n")
				receiver.done <- struct{}{}
				continue
			} else if err != nil {
				fmt.Printf("Error: %v", err)
				time.Sleep(10 * time.Second)
			}
			if msg != nil {
				msgChan <- msg
			}
		}
	}
}

/*
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
		//amqp.LinkSelectorFilter("amqp.annotation.x-opt-offset > '11000'"),
		amqp.LinkSelectorFilter(fmt.Sprintf("amqp.annotation.x-opt-enqueued-time > %v", (time.Now().UnixNano()/1000000))),
	)

	if err != nil {
		return err
	}

	client.receiver = receiver

	return nil

}
*/