package main

import (
	"eventhub/eventhubs"
	"fmt"
	"time"
	"context"
)

const (
	Partitions = 2
	ConsumerGroup = ""
)

func listen(partition int) {
	fmt.Printf("Listening to partition %d\n", partition)

	client := eventhubs.NewClient(&eventhubs.EventHubConfig{
		EventHubNamespace:     "",
		EventHubName:          "",
		EventHubAccessKeyName: "",
		EventHubAccessKey:     "",
	})

	err := client.CreateConnection()
	if err != nil {
		fmt.Printf("Could not create connection: %v", err)
	}

	ctx := context.Background()	

	err = client.Receive(
		ctx, 
		ConsumerGroup, 
		partition, 
		handleMessage, 
		eventhubs.WithTimeEnqueuedFilter(time.Now()),
	)
	if err != nil {
		fmt.Printf("Could not receive messages: %v", err)
	} 
    c := make(chan struct{})
    <-c
	
	client.Close()
}

func handleMessage(ctx context.Context, eventData *eventhubs.EventData) error {
	fmt.Printf("I got my message: %s\n", eventData.Data)
	fmt.Printf("I got my message offset: %s\n", eventData.Offset)
	fmt.Printf("I got my message seq num: %d\n", eventData.SequenceNumber)
	fmt.Printf("I got my message enqueued: %s\n", eventData.EnqueuedTime)
	fmt.Printf("I got my message partition: %s\n", eventData.PartitionKey)
	return nil
}

func handleError(ctx context.Context, err error) {
	fmt.Printf("My message ")
}

func main() {

	for i := 0; i < Partitions; i++ {
		go listen(i)
	}
	
	var input string
	fmt.Scanln(&input)

	fmt.Println("Done")
}
