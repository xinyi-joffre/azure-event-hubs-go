package main

import (
	"eventhub/eventhubs"
	"fmt"
	"bufio"
	"os"
)

func main() {

	client := eventhubs.New(&eventhubs.ConnectionOptions{
		EventHubNamespace:     "eventhubnamespace",
		EventHubName:          "eventhubname",
		EventHubAccessKeyName: "accesskeyname",
		EventHubAccessKey:     "accesskey",
	})

	err := client.CreateConnection()
	if err != nil {
		fmt.Printf("Could not create connection: %v", err)
	}

	err = client.CreateSender()
	if err != nil {
		fmt.Printf("Could not create sender: %v", err)
	}

	fmt.Printf("Type your messages: \n")
	scanner := bufio.NewScanner(os.Stdin)

	
	for {
		if scanner.Scan() {
			line := scanner.Text()
			err = client.Send(line)
			if err != nil {
				fmt.Printf("Could not send message: %v", err)
			}
		}
		
	}
}