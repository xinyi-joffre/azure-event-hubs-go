package eventhubs

import (
	"context"
	"fmt"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"io"
	"math"
	"os"
	"pack.ag/amqp"
	"sync"
	"regexp"
	"time"
)

// EventHub Strings
const (
	EventHubHost          = "amqps://%s.servicebus.windows.net"
	EventHubPublisherPath = "%s/Publishers/%s" // i.e. eventhubname/Publishers/publishername
)

const Forever time.Duration = math.MaxInt64

var (
	connStrRegex = regexp.MustCompile(`Endpoint=sb:\/\/(?P<Host>.+?);SharedAccessKeyName=(?P<KeyName>.+?);SharedAccessKey=(?P<Key>[^;]+?)(?:\z|(?:(?:;EntityPath=)(?P<Entity>.+)))`)
)

// EventHub Main Client
type EventHubClient struct {
	amqpClient *AmqpClient
	context    *context.Context
	session    *amqp.Session
	Config     EventHubConfig
	Logger     io.Writer
	receivers      map[string]*Receiver
	receiver	*Receiver
	sender *Sender
	receiverMu     sync.Mutex
	senderMu       sync.Mutex
}

// Config for Connecting to Event Hub
type EventHubConfig struct {
	EventHubNamespace     string
	EventHubName          string
	EventHubAccessKeyName string
	EventHubAccessKey     string
}

type Handler func(context.Context, *EventData) error

// Create Event Hub Client with Config
func NewClient(config *EventHubConfig) (client *EventHubClient) {
	client = &EventHubClient{
		Config: *config,
		Logger: os.Stdout,
		receivers: make(map[string]*Receiver),
	}
	return client
}

// Create Connection to Event Hub
func (client *EventHubClient) CreateConnection() error {
	host := fmt.Sprintf(EventHubHost, client.Config.EventHubNamespace)
	accessKeyName := client.Config.EventHubAccessKeyName
	accessKey := client.Config.EventHubAccessKey

	amqpClient, err := amqp.Dial(
		host,
		amqp.ConnSASLPlain(accessKeyName, accessKey),
		amqp.ConnIdleTimeout(Forever),
	)

	if err != nil {
		return err
	}

	session, err := amqpClient.NewSession()
	if err != nil {
		return err
	}

	ctx := context.Background()

	client.amqpClient = &AmqpClient{amqpClient}
	client.session = session
	client.context = &ctx

	return err
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

	err := client.amqpClient.Close()
	if err != nil {
		return err
	}

	return nil
}

func (client *EventHubClient) Send(ctx context.Context, eventData *EventData, opts ...SendOption) error {

	sender, err := client.fetchSender()

	if err != nil {
		return err
	}

	return sender.Send(ctx, eventData, opts...)
}

func (client *EventHubClient) Receive(ctx context.Context, consumerGroup string, partition int, handler Handler, opts ...ReceiveOption) error {
	receiver, err := NewReceiver(client.amqpClient, client.Config.EventHubName, consumerGroup, partition, opts...)
	if err != nil {
		return err
	}

	receiver.Listen(handler)

	client.receiver = receiver
	return nil
}

func (client *EventHubClient) ReceiveEvent(ctx context.Context, consumerGroup string, partition int, opts ...ReceiveOption) error {
	receiver, err := NewReceiver(client.amqpClient, client.Config.EventHubName, consumerGroup, partition, opts...)
	if err != nil {
		return err
	}

	_, err = receiver.Receive()
	if err != nil {
		return err
	}

	client.receiver = receiver	
	
	return nil
}

func (client *EventHubClient) fetchSender() (*Sender, error) {
	client.senderMu.Lock()
	defer client.senderMu.Unlock()

	if client.sender != nil {
		return client.sender, nil
	}
	fmt.Println("Inside Mutex 2")
	sender, err := NewSender(client.amqpClient, client.Config.EventHubName)
	if err != nil {
		return nil, err
	}

	client.sender = sender

	return sender, nil
}

func (client *EventHubClient) fetchReceiver(consumerGroup string, partition int, opts ...ReceiveOption) (*Receiver, error) {
	client.receiverMu.Lock()
	defer client.receiverMu.Unlock()

	target := fmt.Sprintf("%s/%d", consumerGroup, partition)

	receiver, ok := client.receivers[target]
	if ok {
		return receiver, nil
	}
	
	fmt.Println("Inside Mutex 3")
	receiver, err := NewReceiver(client.amqpClient, client.Config.EventHubName, consumerGroup, partition, opts...)
	if err != nil {
		return nil, err
	}

	client.receivers[target] = receiver

	return receiver, nil
}

// NewWithMSI creates a new connected instance of an Azure Event Hub given a subscription Id, resource group,
// Event Hub namespace, and Event Hub authorization rule name.
func NewWithMSI(subscriptionID, resourceGroup, namespace, eventHubName, accessKeyName string, environment azure.Environment) (*EventHubClient, error) {
	msiEndpoint, err := adal.GetMSIVMEndpoint()
	spToken, err := adal.NewServicePrincipalTokenFromMSI(msiEndpoint, environment.ResourceManagerEndpoint)

	if err != nil {
		return nil, err
	}

	return NewWithSPToken(spToken, subscriptionID, resourceGroup, namespace, eventHubName, accessKeyName, environment)
}

// NewWithSPToken creates a new connected instance of an Azure Event Hub given a, Azure Active Directory service
// principal token subscription Id, resource group, Event Hub namespace, and Event Hub Access Key name.
func NewWithSPToken(spToken *adal.ServicePrincipalToken, subscriptionID, resourceGroup,
	namespace, eventHubName, accessKeyName string, environment azure.Environment) (*EventHubClient, error) {

	authorizer := autorest.NewBearerAuthorizer(spToken)

	ehClient := mgmt.NewEventHubsClientWithBaseURI(environment.ResourceManagerEndpoint, subscriptionID)
	ehClient.Authorizer = authorizer
	ehClient.AddToUserAgent("eventhub")

	result, err := ehClient.ListKeys(context.Background(), resourceGroup, namespace, eventHubName, accessKeyName)
	if err != nil {
		return nil, err
	}

	accessKey := *result.PrimaryKey
	client := NewClient(
		&EventHubConfig{
			EventHubNamespace:     namespace,
			EventHubName:          eventHubName,
			EventHubAccessKeyName: accessKeyName,
			EventHubAccessKey:     accessKey,
		},
	)
	if err != nil {
		return nil, err
	}

	return client, err
}
