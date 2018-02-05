package eventprocessor

import (
	"context"
)

type EventProcessor interface {
	Close(context *context.Context, reason string) error
	Open(context *context.Context) error
	ProcessError(context *context.Context) error
	ProcessEvents(context *context.Context, messages []string) error //take care of checkpointing
}

type PartitionManager interface {
	GetPartitionIds() //get from rest endpoint https://_.servicebus.windows.net/ehname?timeout=60&api-version=2014-01
	Start() //Initialize partition checkpoint and lease store and then calls run
	Stop() //Stops the partition manager
	Run() //Starts loop to run all partition readers, and then cleans up all partition readers when down
	InitializeStore() //Initialize partition checkpoint and lease store
	Retry() // attempt to renew lease
	RunLoopAsync() //gets possible leases and acquires them, then tries to steal leases
	CheckAndAddPumpAsync(partitionId int) //updates lease on existing pump, or starts new one
	CreateNewPump(partitionId int, lease string) //create new pump
	RemovePumpAsync(partitionId int, reason string) //tries to remove pump on certain partition
}

type CheckpointManager interface {

}

type LeaseManager interface {
	
}