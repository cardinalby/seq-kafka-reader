package tracking

import (
	"fmt"
	"strings"
	"sync"

	"cardinalby/seq-kafka-reader/types"
	"github.com/gammazero/deque"
)

// PartitionTracker tracks offset for a single partition, tracking several offset sequences if needed
// (in case of offset shifts or re-balances)
// It's safe to call the methods from different goroutines
type PartitionTracker struct {
	seqTrackers        *deque.Deque[*SequenceTracker]
	seqTrackersMu      sync.RWMutex
	commitResBroadcast *CommitResultBroadcast
}

func NewPartitionTracker(firstFetched int64) *PartitionTracker {
	res := &PartitionTracker{
		seqTrackers:        deque.New[*SequenceTracker](2),
		commitResBroadcast: newCommitResultBroadcast(),
	}
	res.seqTrackers.PushFront(NewSequenceTracker(firstFetched))
	return res
}

// OnFetch informs the tracker that the offset has been fetched
func (t *PartitionTracker) OnFetch(o int64) {
	tryToTrack := func() bool {
		tracked := false
		for i := 0; i < t.seqTrackers.Len(); i++ {
			if err := t.seqTrackers.At(i).OnFetch(o); err == nil {
				tracked = true
			}
		}
		return tracked
	}

	t.seqTrackersMu.RLock()
	tracked := tryToTrack()
	t.seqTrackersMu.RUnlock()
	if !tracked {
		t.seqTrackersMu.Lock()
		if !tryToTrack() {
			t.seqTrackers.PushFront(NewSequenceTracker(o))
		}
		t.seqTrackersMu.Unlock()
	}
}

// OnCommitIntent informs the tracker that the offset can be committed once previous offsets are ready
// Returns an error if all tracked sequences don't expect commit intent for this offset
func (t *PartitionTracker) OnCommitIntent(o int64) error {
	t.seqTrackersMu.RLock()
	defer t.seqTrackersMu.RUnlock()

	var errors []error
	for i := 0; i < t.seqTrackers.Len(); i++ {
		if _, err := t.seqTrackers.At(i).OnCommitIntent(o); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) == t.seqTrackers.Len() {
		return fmt.Errorf("%w: %s", types.ErrUnexpectedOffset, joinErrorStrings(errors))
	}
	return nil
}

// PopNextToCommit extracts the highest offset that can be committed
func (t *PartitionTracker) PopNextToCommit() int64 {
	t.seqTrackersMu.RLock()
	defer t.seqTrackersMu.RUnlock()

	maxToCommit := int64(-1)
	for i := 0; i < t.seqTrackers.Len(); i++ {
		if toCommit := t.seqTrackers.At(i).PopNextToCommit(); toCommit > maxToCommit {
			maxToCommit = toCommit
		}
	}
	return maxToCommit
}

// PeepNextToCommit returns the highest offset that can be committed without removing (is idempotent)
func (t *PartitionTracker) PeepNextToCommit() int64 {
	t.seqTrackersMu.RLock()
	defer t.seqTrackersMu.RUnlock()

	maxToCommit := int64(-1)
	for i := 0; i < t.seqTrackers.Len(); i++ {
		if toCommit := t.seqTrackers.At(i).PeepNextToCommit(); toCommit > maxToCommit {
			maxToCommit = toCommit
		}
	}
	return maxToCommit
}

// OnCommitResult marks the offset as successfully committed for all sequences, removing completed sequences if needed
// Returns an error if there is no sequence that expects that committed offset
func (t *PartitionTracker) OnCommitResult(o int64, err error) error {
	t.seqTrackersMu.RLock()

	hasCorrespondingTracker := false
	for i := 0; i < t.seqTrackers.Len(); i++ {
		if o <= t.seqTrackers.At(i).GetMaxInProgress() {
			hasCorrespondingTracker = true
			break
		}
	}
	if !hasCorrespondingTracker {
		t.seqTrackersMu.RUnlock()
		// since the commit result was rejected, don't notify subscribers
		return fmt.Errorf(
			"%w: there are no tracked sequences awaiting commit result at %d",
			types.ErrUnexpectedOffset, o,
		)
	}
	// without W lock we can't guarantee there is still a corresponding tracker, but it's not important
	// because this error is mostly for tests and logging purposes, and it should never happen with a proper
	// use of PartitionTracker

	var seqTrackersToRemove []*SequenceTracker
	for i := 0; i < t.seqTrackers.Len(); i++ {
		seqTracker := t.seqTrackers.At(i)
		seqTracker.OnCommitResult(o, err)
		if i != 0 && seqTracker.IsDone() {
			// if it's not the first (most recently added) tracker, and it's done
			// it most probably can be removed because it will not receive
			// any new fetches (if it will, it will be safely recreated)
			// The first tracker will probably receive new fetches
			seqTrackersToRemove = append(seqTrackersToRemove, seqTracker)
		}
	}
	t.seqTrackersMu.RUnlock()

	if len(seqTrackersToRemove) > 0 {
		shouldRemove := func(item *SequenceTracker) bool {
			for _, tr := range seqTrackersToRemove {
				// recheck that it's still need to be removed -
				// another thread can intervene between RUnlock() and Lock()
				if tr == item && tr.IsDone() {
					return true
				}
			}
			return false
		}
		t.seqTrackersMu.Lock()
		defer t.seqTrackersMu.Unlock()

		for i := 0; i < t.seqTrackers.Len(); i++ {
			if shouldRemove(t.seqTrackers.At(i)) {
				t.seqTrackers.Remove(i)
				i-- // elements in seqTrackers were shifted to the left, repeat with the same i
			}
		}
	}
	t.commitResBroadcast.notify(o, err)
	return nil
}

// SubscribeToCommitResult subscribes to commit result calls
func (t *PartitionTracker) SubscribeToCommitResult(offset int64) *MsgResultSubscription {
	t.seqTrackersMu.RLock()
	maxCommitted := int64(-1)
	for i := 0; i < t.seqTrackers.Len(); i++ {
		if committed := t.seqTrackers.At(i).GetCommitted(); committed > maxCommitted {
			maxCommitted = committed
		}
	}

	if offset > maxCommitted {
		defer t.seqTrackersMu.RUnlock()
		return t.commitResBroadcast.subscribe(offset)
	}

	t.seqTrackersMu.RUnlock()
	// is already committed, return chan with success
	return CreateNewSuccessMsgResultSubscription()
}

func joinErrorStrings(errs []error) string {
	sb := strings.Builder{}
	for i, err := range errs {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(err.Error())
	}
	return sb.String()
}
