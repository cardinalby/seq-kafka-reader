package tracking

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var commitErr = errors.New("commit err")

func (ct *SequenceTracker) requireAddIntent(t *testing.T, o int64, requireAdded bool, requireErr bool) {
	t.Helper()
	added, err := ct.OnCommitIntent(o)
	if requireErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
	require.Equal(t, requireAdded, added, "'added' return value doesn't meet expectations")
}

func TestSequenceTrackerWithGaps(t *testing.T) {
	t.Parallel()
	tr := NewSequenceTracker(2)

	require.EqualValues(t, 1, tr.GetCommitted())

	tr.requireAddIntent(t, 2, true, false)
	require.EqualValues(t, 2, tr.PeepNextToCommit())
	require.EqualValues(t, -1, tr.GetMaxInProgress())

	require.NoError(t, tr.OnFetch(3))
	tr.requireAddIntent(t, 3, true, false)
	require.EqualValues(t, 3, tr.PeepNextToCommit())
	require.EqualValues(t, -1, tr.GetMaxInProgress())
	require.EqualValues(t, 3, tr.PopNextToCommit())
	require.EqualValues(t, 3, tr.GetMaxInProgress())
	require.EqualValues(t, 1, tr.GetCommitted())
	require.EqualValues(t, -1, tr.PopNextToCommit())
	require.EqualValues(t, 3, tr.GetMaxInProgress())
	require.EqualValues(t, []int64(nil), tr.awaitingIntents.OffsetKeys())

	require.NoError(t, tr.OnFetch(4))
	require.NoError(t, tr.OnFetch(5))
	require.NoError(t, tr.OnFetch(6))

	tr.requireAddIntent(t, 5, true, false)
	require.EqualValues(t, -1, tr.PopNextToCommit())
	tr.requireAddIntent(t, 6, true, false)
	require.EqualValues(t, -1, tr.PopNextToCommit())
	require.EqualValues(t, 3, tr.GetMaxInProgress())
	require.EqualValues(t, 1, tr.GetCommitted())
	require.EqualValues(t, []int64{5, 6}, tr.awaitingIntents.OffsetKeys())

	tr.requireAddIntent(t, 4, true, false)
	require.EqualValues(t, 6, tr.PopNextToCommit())
	require.EqualValues(t, []int64(nil), tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 1, tr.GetCommitted())
	require.EqualValues(t, 6, tr.GetMaxInProgress())

	tr.OnCommitResult(3, nil)
	require.EqualValues(t, 3, tr.GetCommitted())
	require.EqualValues(t, 6, tr.GetMaxInProgress())
	require.EqualValues(t, -1, tr.PeepNextToCommit())

	tr.OnCommitResult(2, nil)
	require.EqualValues(t, 3, tr.GetCommitted())
	require.EqualValues(t, 6, tr.GetMaxInProgress())

	tr.OnCommitResult(6, nil)
	require.EqualValues(t, 6, tr.GetCommitted())
	require.EqualValues(t, -1, tr.GetMaxInProgress())
	require.EqualValues(t, -1, tr.PopNextToCommit())
}

func TestSequenceTrackerWithWrongOrder(t *testing.T) {
	t.Parallel()
	tr := NewSequenceTracker(10)

	tr.requireAddIntent(t, 10, true, false)
	require.EqualValues(t, 10, tr.PopNextToCommit())

	tr.requireAddIntent(t, 10, false, false)
	require.EqualValues(t, 10, tr.GetMaxInProgress())
	require.EqualValues(t, 9, tr.GetCommitted())

	require.NoError(t, tr.OnFetch(11))
	require.EqualValues(t, 11, tr.GetLastFetch())

	require.Error(t, tr.OnFetch(10))
	require.NoError(t, tr.OnFetch(12))

	tr.requireAddIntent(t, 13, false, true)
	tr.requireAddIntent(t, 12, true, false)
	require.EqualValues(t, -1, tr.PopNextToCommit())
	require.EqualValues(t, []int64{12}, tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 9, tr.GetCommitted())
	require.EqualValues(t, 10, tr.GetMaxInProgress())

	tr.requireAddIntent(t, 8, false, false)
	tr.requireAddIntent(t, 8, false, false)
	require.EqualValues(t, []int64{12}, tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 9, tr.GetCommitted())
	require.EqualValues(t, 10, tr.GetMaxInProgress())

	tr.OnCommitResult(9, nil)
	require.EqualValues(t, []int64{12}, tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 9, tr.GetCommitted())
	require.EqualValues(t, 10, tr.GetMaxInProgress())

	tr.OnCommitResult(11, nil)
	require.EqualValues(t, []int64(nil), tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 11, tr.GetCommitted())
	require.EqualValues(t, -1, tr.GetMaxInProgress())
	require.EqualValues(t, 12, tr.PeepNextToCommit())

	tr.OnCommitResult(13, nil)
	require.EqualValues(t, []int64(nil), tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 13, tr.GetCommitted())
	require.EqualValues(t, -1, tr.GetMaxInProgress())
	require.EqualValues(t, -1, tr.PeepNextToCommit())

	tr.requireAddIntent(t, 8, false, false)
	require.EqualValues(t, []int64(nil), tr.awaitingIntents.OffsetKeys())
}

func TestSequenceTracker_OnCommitSuccess(t *testing.T) {
	t.Parallel()
	tr := NewSequenceTracker(1)

	require.NoError(t, tr.OnFetch(2))
	tr.requireAddIntent(t, 2, true, false)
	tr.requireAddIntent(t, 1, true, false)
	require.EqualValues(t, 2, tr.PopNextToCommit())

	tr.OnCommitResult(2, nil)
	tr.OnCommitResult(1, nil)
}

func TestSequenceTracker_OnCommitFailed(t *testing.T) {
	t.Parallel()
	tr := NewSequenceTracker(1)
	require.NoError(t, tr.OnFetch(2))

	tr.requireAddIntent(t, 1, true, false)
	tr.requireAddIntent(t, 2, true, false)
	require.EqualValues(t, 2, tr.PopNextToCommit())
	require.EqualValues(t, 2, tr.GetMaxInProgress())
	require.EqualValues(t, -1, tr.PeepNextToCommit())
	require.NoError(t, tr.OnFetch(3))

	tr.requireAddIntent(t, 3, true, false)
	require.EqualValues(t, 3, tr.PopNextToCommit())
	require.EqualValues(t, 3, tr.GetMaxInProgress())

	tr.OnCommitResult(1, commitErr) // ignored
	require.EqualValues(t, 3, tr.GetMaxInProgress())
	require.EqualValues(t, -1, tr.PeepNextToCommit())

	tr.OnCommitResult(3, commitErr)
	require.EqualValues(t, 0, tr.GetCommitted())
	require.EqualValues(t, -1, tr.GetMaxInProgress())
	require.EqualValues(t, 3, tr.PeepNextToCommit())

	tr.OnCommitResult(2, commitErr)
	require.EqualValues(t, -1, tr.GetMaxInProgress())
	require.EqualValues(t, 3, tr.PeepNextToCommit())
}

func TestSequenceTracker_WrongPopsWhenThereIsNoSequence(t *testing.T) {
	t.Parallel()
	tr := NewSequenceTracker(1)

	require.NoError(t, tr.OnFetch(2))
	require.NoError(t, tr.OnFetch(3))
	require.NoError(t, tr.OnFetch(4))
	tr.requireAddIntent(t, 4, true, false)
	require.EqualValues(t, []int64{4}, tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, -1, tr.PopNextToCommit())
	tr.requireAddIntent(t, 3, true, false)
	require.EqualValues(t, []int64{3, 4}, tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, -1, tr.PopNextToCommit())
	tr.requireAddIntent(t, 1, true, false)
	require.EqualValues(t, []int64{3, 4}, tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 1, tr.PopNextToCommit())
	tr.requireAddIntent(t, 2, true, false)
	require.EqualValues(t, []int64(nil), tr.awaitingIntents.OffsetKeys())
	require.EqualValues(t, 4, tr.PopNextToCommit())
}

func BenchmarkNewSequenceTracker(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tr := NewSequenceTracker(0)
		wg := sync.WaitGroup{}
		for o := int64(1); o <= 2000; o++ {
			o := o
			_ = tr.OnFetch(o)
			wg.Add(1)
			go func() {
				time.Sleep(1)
				_, _ = tr.OnCommitIntent(o)
				if toCommit := tr.PopNextToCommit(); toCommit != -1 {
					tr.OnCommitResult(o, nil)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
