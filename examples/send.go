package main

import (
	"bufio"
	"context"
	"eventhub/eventhubs"
	"fmt"
	"os"
)

func main() {

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

	fmt.Printf("Type your messages: \n")
	scanner := bufio.NewScanner(os.Stdin)

	ctx := context.Background()

	for {
		if scanner.Scan() {
			line := scanner.Text()
			err = client.Send(ctx, &eventhubs.EventData{
				Data: []byte(line),
			})
			if err != nil {
				fmt.Printf("Could not send message: %v", err)
			}
		}
	}
}
