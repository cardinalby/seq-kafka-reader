package tracking

import (
	"fmt"
	"sync"
)

// SequenceTracker keeps records about single sequence of offsets in one partition.
// It stores commit intents that come out of sequence, allowing you to get the highest offset that can be
// committed (when all commit intents starting from already committed are present)
// It's safe to use it from multiple goroutines
type SequenceTracker struct {
	lastFetched int64
	// reads from: OnCommitIntent, GetLastFetch; writes: OnFetch
	// separating it from commitsMu can increase performance allowing more granular locking
	// if we remove checks of offset correctness in methods, it can be extended even more
	lastFetchedMu sync.RWMutex

	nextToCommit    int64
	maxInProgress   int64
	committed       int64
	awaitingIntents *OffsetSortedMap[bool] // awaiting offsets
	// locks any operations on all fields above responsible for tracking commits (opposite to fetches)
	commitsMu sync.RWMutex
}

func NewSequenceTracker(firstFetched int64) *SequenceTracker {
	res := &SequenceTracker{
		lastFetched:     firstFetched,
		nextToCommit:    -1,
		maxInProgress:   -1,
		committed:       firstFetched - 1,
		awaitingIntents: NewOffsetFlags(),
	}
	return res
}

// OnFetch informs the tracker about new fetch. Fetches must always increase by 1
func (ct *SequenceTracker) OnFetch(o int64) error {
	ct.lastFetchedMu.Lock()
	defer ct.lastFetchedMu.Unlock()

	if o != ct.lastFetched+1 {
		return fmt.Errorf("fetch at %d, but last fetched = %d", o, ct.lastFetched)
	}

	ct.lastFetched = o
	return nil
}

// OnCommitIntent informs the tracker that offset is ready to commit
func (ct *SequenceTracker) OnCommitIntent(o int64) (added bool, err error) {
	ct.lastFetchedMu.RLock()
	if o > ct.lastFetched {
		ct.lastFetchedMu.RUnlock()
		return false, fmt.Errorf("intent commit at %d > last fetched = %d", o, ct.lastFetched)
	}
	ct.lastFetchedMu.RUnlock()

	ct.commitsMu.Lock()
	defer ct.commitsMu.Unlock()

	theHighestReady := ct.getTheHighestReady()
	if o <= theHighestReady {
		return false, nil
	}

	// offset right before o is ready, it means o is also ready
	if o-1 == theHighestReady {
		ct.nextToCommit = o
		// next offset after o is in awaitingIntents, it means o makes a sequence of
		// offsets ready to commit: o, o+1, and maybe more
		if ct.awaitingIntents.MinOffset() == o+1 {
			ct.nextToCommit += int64(ct.awaitingIntents.PopMinUntilSequenceEnd())
		}
		return true, nil
	}

	ct.awaitingIntents.Set(o, true)
	return true, nil
}

// OnCommitResult marks all offsets <= o as committed and not in progress anymore
func (ct *SequenceTracker) OnCommitResult(o int64, err error) {
	ct.commitsMu.Lock()
	defer ct.commitsMu.Unlock()

	if o <= ct.committed {
		return // do nothing, a higher offset has been already committed
	}

	if err == nil {
		ct.committed = o
		ct.awaitingIntents.RemoveLessOrEqual(o)
		if o >= ct.nextToCommit { // commit result came from another sequence
			// if next offset is ready, it means it makes a sequence of ready offsets starting from o, o+1, ...?
			if ct.awaitingIntents.MinOffset() == o+1 {
				ct.nextToCommit = o + int64(ct.awaitingIntents.PopMinUntilSequenceEnd())
			} else {
				ct.nextToCommit = -1
			}
		}
	} else if o == ct.maxInProgress && ct.nextToCommit == -1 {
		// ignore the case when o < ct.maxInProgress, because it can't be properly handled,
		// wait for ct.maxInProgress offset commit result
		ct.nextToCommit = o
	}
	if o >= ct.maxInProgress {
		ct.maxInProgress = -1
	}

	return
}

// PopNextToCommit returns the next offset that can be committed if any or -1. It marks the returned offset as
// maxInProgress. Next calls will return -1 until new ready offsets are added with OnCommitIntent() and make a sequence
func (ct *SequenceTracker) PopNextToCommit() int64 {
	ct.commitsMu.Lock()
	defer ct.commitsMu.Unlock()

	res := ct.nextToCommit
	ct.nextToCommit = -1
	if res != -1 {
		ct.maxInProgress = res
	}
	return res
}

// PeepNextToCommit returns the next offset that can be committed if any or -1
func (ct *SequenceTracker) PeepNextToCommit() int64 {
	return ct.nextToCommit
}

// GetLastFetch returns the highest recorded fetch offset or -1 if there were no fetches
func (ct *SequenceTracker) GetLastFetch() int64 {
	return ct.lastFetched
}

// GetMaxInProgress returns the highest offset in commit progress if any or -1
// Normally it's the value of a previous PopNextToCommit() call or -1 if it was successfully committed
func (ct *SequenceTracker) GetMaxInProgress() int64 {
	return ct.maxInProgress
}

// GetCommitted returns the last offset that has been successfully committed if any or -1
func (ct *SequenceTracker) GetCommitted() int64 {
	return ct.committed
}

// IsDone returns true if no new commit intents or commit result are expected and
// all awaiting intents were committed
func (ct *SequenceTracker) IsDone() bool {
	ct.lastFetchedMu.RLock()
	defer ct.lastFetchedMu.RUnlock()
	ct.commitsMu.RLock()
	defer ct.commitsMu.RUnlock()

	return ct.nextToCommit == -1 &&
		ct.committed >= ct.lastFetched &&
		ct.maxInProgress == -1 &&
		ct.awaitingIntents.IsEmpty()
}

// getTheHighestReady returns max offset among nextToCommit, maxInProgress, committed
func (ct *SequenceTracker) getTheHighestReady() int64 {
	if ct.nextToCommit != -1 { // if exists, it's the highest offset
		return ct.nextToCommit
	}
	if ct.maxInProgress != -1 { // take this if exists (it's > than ct.committed)
		return ct.maxInProgress
	}
	return ct.committed
}
