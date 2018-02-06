package eventhubs

import (
	"pack.ag/amqp"
	"time"
)

const (
	EnqueuedDateTimeLayout = "2018-02-03 05:43:56.559 +0000 UTC"	
)

type EventData struct {
	SequenceNumber int64
	Offset         string
	EnqueuedTime   time.Time
	PartitionKey   string
	Data           []byte
}

func UnpackAmqpMessage(message *amqp.Message) (*EventData, error) {

	var partitionKey string
	value, ok := message.Annotations["x-opt-partition-key"]

	if ok {
		partitionKey = value.(string)
	}

	return &EventData{
		SequenceNumber: int64(message.Annotations["x-opt-sequence-number"].(int64)),		
		EnqueuedTime: message.Annotations["x-opt-enqueued-time"].(time.Time),		
		Offset: message.Annotations["x-opt-offset"].(string),
		PartitionKey: partitionKey,
		Data: message.Data,
	}, nil
}

func (eventData *EventData) String() string {
	return string(eventData.Data)
}
