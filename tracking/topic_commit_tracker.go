package tracking

import (
	"fmt"
	"sync"

	"cardinalby/seq-kafka-reader/types"
	"github.com/segmentio/kafka-go"
)

// TopicCommitTracker tracks offsets for all consuming partitions of a topic.
// It's safe to call the methods from different goroutines
type TopicCommitTracker struct {
	partitionTrackers   map[int]*PartitionTracker
	partitionTrackersMu sync.RWMutex // to synchronize partitionTrackers keys read/write
}

func NewTopicCommitTracker() *TopicCommitTracker {
	return &TopicCommitTracker{
		partitionTrackers: make(map[int]*PartitionTracker),
	}
}

// OnCommitIntent informs the tracker that messages can be committed once all
// previous offsets in their partitions are ready as well
func (tr *TopicCommitTracker) OnCommitIntent(messages []kafka.Message) types.MessageErrors {
	tr.partitionTrackersMu.RLock()
	defer tr.partitionTrackersMu.RUnlock()

	var commitErr types.MessageErrors
	for i := range messages {
		if partitionTracker := tr.partitionTrackers[messages[i].Partition]; partitionTracker != nil {
			if err := partitionTracker.OnCommitIntent(messages[i].Offset); err != nil {
				commitErr = append(commitErr, types.MessageError{
					Message: &messages[i],
					Err: fmt.Errorf("error adding commit intent for partition %d: %w",
						messages[i].Partition, err),
				})
			}
		} else {
			commitErr = append(commitErr, types.MessageError{
				Message: &messages[i],
				Err: fmt.Errorf(
					"%w: commit intent (partition: %d, offset: %d) before any fetches for the partition",
					types.ErrUnexpectedOffset, messages[i].Partition, messages[i].Offset,
				),
			})
		}
	}
	return commitErr
}

func (tr *TopicCommitTracker) SubscribeToCommitResult(messages []kafka.Message) MessageResultSubs {
	subs := make(MessageResultSubs, len(messages))
	tr.partitionTrackersMu.RLock()
	for i := range messages {
		// all trackers should have been already created because normally these messages should have been fetched first
		// check, don't subscribe to detect a possible error
		if _, ok := tr.partitionTrackers[messages[i].Partition]; !ok {
			return nil
		}
		subs[&messages[i]] = tr.partitionTrackers[messages[i].Partition].SubscribeToCommitResult(messages[i].Offset)
	}
	tr.partitionTrackersMu.RUnlock()
	return subs
}

// OnCommitResult informs the tracker about the Result of a commit operation
func (tr *TopicCommitTracker) OnCommitResult(message *kafka.Message, err error) error {
	tr.partitionTrackersMu.RLock()
	partitionTracker := tr.partitionTrackers[message.Partition]
	tr.partitionTrackersMu.RUnlock()
	if partitionTracker == nil {
		return fmt.Errorf(
			"%w: commit result (partition: %d, offset: %d) before any fetches for the partition",
			types.ErrUnexpectedOffset, message.Partition, message.Offset,
		)
	}
	return partitionTracker.OnCommitResult(message.Offset, err)
}

// OnFetch informs the tracker about a fetch from the partition.
// First fetch for a partition us used to set starting committed offset = offset-1
func (tr *TopicCommitTracker) OnFetch(message *kafka.Message) {
	tr.partitionTrackersMu.RLock()
	partitionTracker := tr.partitionTrackers[message.Partition]
	tr.partitionTrackersMu.RUnlock()

	if partitionTracker == nil { // it's the first operation for the partition
		// partitionTracker need to be created, lock map for write
		tr.partitionTrackersMu.Lock()
		// check again: partition partitionTracker may have appeared at the moment between
		// partitionTrackersMu.RUnlock() and partitionTrackersMu.Lock()
		partitionTracker = tr.partitionTrackers[message.Partition]
		if partitionTracker == nil {
			partitionTracker = NewPartitionTracker(message.Offset)
			tr.partitionTrackers[message.Partition] = partitionTracker
		}
		tr.partitionTrackersMu.Unlock()
	} else {
		partitionTracker.OnFetch(message.Offset)
	}
}

// PopNextToCommit returns map of [partition]offsetToCommit for all known partitions or nil if none have
// offsets to commit. Can be used if you perform commit by timer
func (tr *TopicCommitTracker) PopNextToCommit() []kafka.Message {
	tr.partitionTrackersMu.RLock()
	defer tr.partitionTrackersMu.RUnlock()

	var res []kafka.Message
	for partition, tracker := range tr.partitionTrackers {
		if offset := tracker.PopNextToCommit(); offset != -1 {
			res = append(res, kafka.Message{Partition: partition, Offset: offset})
		}
	}
	return res
}
