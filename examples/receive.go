package main

import (
	"eventhub/eventhubs"
	"fmt"
)

func listen(partition int) {
	fmt.Println("inside")

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
	err = client.CreateReceiver("testconsumer", partition)
	if err != nil {
		fmt.Printf("Could not create receiver: %v", err)
	}

	err = client.Receive()
	if err != nil {
		fmt.Printf("Could not receive messages: %v", err)
	}

	client.Close()
}

func main() {
	fmt.Println("Hello")
	go listen(0)
	go listen(1)

	var input string
	fmt.Scanln(&input)

	fmt.Println("Done")
}
