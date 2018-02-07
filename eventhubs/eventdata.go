package eventhubs

import (
	"qpid.apache.org/amqp"	
	"time"
)

type EventData struct {
	SequenceNumber int64
	Offset         string
	EnqueuedTime   time.Time
	PartitionKey   string
	Data           []byte
}

func UnpackAmqpMessage(message amqp.Message) (*EventData, error) {

	var partitionKey string
	value, ok := message.MessageAnnotations()[amqp.AnnotationKeyString("x-opt-partition-key")]

	if ok {
		partitionKey = value.(string)
	}

	eventData := &EventData{
		SequenceNumber: message.MessageAnnotations()[amqp.AnnotationKeyString("x-opt-sequence-number")].(int64),		
		EnqueuedTime: message.MessageAnnotations()[amqp.AnnotationKeyString("x-opt-enqueued-time")].(time.Time),		
		Offset: message.MessageAnnotations()[amqp.AnnotationKeyString("x-opt-offset")].(string),
		PartitionKey: partitionKey,
		Data: []byte(message.Body().(amqp.Binary)),
	}

	eventData.EnqueuedTime = time.Unix(eventData.EnqueuedTime.Unix()*1000, 0).UTC()

	return eventData, nil
}

func (eventData *EventData) String() string {
	return string(eventData.Data)
}
