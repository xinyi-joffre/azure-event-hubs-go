package eventhubs

/*
import (
	"os"
	"io"
	"log"
	"crypto/tls"
)
*/

type AmqpClient interface {
	New(url string)
	Send(message string)
	SendAsync(message string)
}
