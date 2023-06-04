package tracking

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_PartitionsSequenceTracker_ShiftOffsetForward(t *testing.T) {
	t.Parallel()
	tr := NewPartitionTracker(1)

	require.NoError(t, tr.OnCommitIntent(1))
	require.EqualValues(t, 1, tr.PopNextToCommit())
	tr.OnFetch(2)
	tr.OnFetch(3)
	require.NoError(t, tr.OnCommitIntent(3))
	require.EqualValues(t, -1, tr.PopNextToCommit())
	require.EqualValues(t, 1, tr.seqTrackers.Len())
	// shifting offset to 10
	tr.OnFetch(10)
	require.EqualValues(t, 2, tr.seqTrackers.Len())
	require.NoError(t, tr.OnCommitIntent(10))
	require.NoError(t, tr.OnCommitIntent(4))
	require.NoError(t, tr.OnCommitResult(1, nil))
	require.EqualValues(t, 10, tr.PopNextToCommit())

	require.NoError(t, tr.OnCommitResult(10, nil))
	require.EqualValues(t, 1, tr.seqTrackers.Len())
}

func Test_PartitionsSequenceTracker_ShiftOffsetHalfwayForward(t *testing.T) {
	t.Parallel()
	tr := NewPartitionTracker(1)

	require.NoError(t, tr.OnCommitIntent(1))
	require.EqualValues(t, 1, tr.PopNextToCommit())
	tr.OnFetch(2)
	tr.OnFetch(3)
	tr.OnFetch(4)
	tr.OnFetch(5)
	require.NoError(t, tr.OnCommitIntent(5))
	require.EqualValues(t, -1, tr.PopNextToCommit())
	require.EqualValues(t, 1, tr.seqTrackers.Len())
	// shifting offset to 3
	tr.OnFetch(3)
	require.EqualValues(t, 2, tr.seqTrackers.Len())
	require.NoError(t, tr.OnCommitIntent(2))
	require.NoError(t, tr.OnCommitIntent(3))
	require.NoError(t, tr.OnCommitIntent(4))
	require.NoError(t, tr.OnCommitIntent(5))
	require.NoError(t, tr.OnCommitResult(1, nil))
	require.EqualValues(t, 5, tr.PopNextToCommit())

	require.NoError(t, tr.OnCommitResult(3, nil))
	require.EqualValues(t, 2, tr.seqTrackers.Len())
	require.NoError(t, tr.OnCommitResult(5, nil))
	require.EqualValues(t, 1, tr.seqTrackers.Len())
}

func Test_PartitionsSequenceTracker_ShiftOffsetBackward(t *testing.T) {
	t.Parallel()
	tr := NewPartitionTracker(10)
	tr.OnFetch(11)

	require.EqualValues(t, 1, tr.seqTrackers.Len())
	require.NoError(t, tr.OnCommitIntent(11))

	// shifting offset to 1
	tr.OnFetch(1)
	require.EqualValues(t, 2, tr.seqTrackers.Len())
	require.EqualValues(t, 1, tr.seqTrackers.At(0).GetLastFetch())
	require.EqualValues(t, 11, tr.seqTrackers.At(1).GetLastFetch())
	require.NoError(t, tr.OnCommitIntent(1))
	require.EqualValues(t, 1, tr.PopNextToCommit())
	tr.OnFetch(2)
	tr.OnFetch(3)
	require.NoError(t, tr.OnCommitIntent(3))
	require.EqualValues(t, -1, tr.PopNextToCommit())
	require.NoError(t, tr.OnCommitIntent(2))
	require.EqualValues(t, 3, tr.PopNextToCommit())
	require.False(t, tr.seqTrackers.At(0).IsDone())
	require.False(t, tr.seqTrackers.At(1).IsDone())

	require.Error(t, tr.OnCommitResult(20, nil))
	require.NoError(t, tr.OnCommitResult(3, nil))
	require.True(t, tr.seqTrackers.At(0).IsDone())
	require.EqualValues(t, 2, tr.seqTrackers.Len())
	tr.OnFetch(4)
	require.NoError(t, tr.OnCommitIntent(4))
	require.EqualValues(t, 4, tr.seqTrackers.At(0).PeepNextToCommit())

	require.Error(t, tr.OnCommitResult(11, nil))
	require.EqualValues(t, 4, tr.PeepNextToCommit())

	require.NoError(t, tr.OnCommitIntent(10))
	require.EqualValues(t, 11, tr.PopNextToCommit())
	require.NoError(t, tr.OnCommitResult(11, nil))

	require.EqualValues(t, 1, tr.seqTrackers.Len())
	require.EqualValues(t, 4, tr.seqTrackers.At(0).GetLastFetch())
}

func requireNoResult[T any](t *testing.T, ch chan T) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if ok {
			t.Errorf("Result in sub when it's not expected: %v", v)
		}
	default:
	}
}

func requireResult[T any](t *testing.T, ch chan T, res any) {
	t.Helper()
	select {
	case v := <-ch:
		require.EqualValues(t, res, v)
	default:
		t.Errorf("no Result in sub")
	}
}

func TestSequenceTracker_SubscribeToCommitResult(t *testing.T) {
	t.Parallel()
	tr := NewPartitionTracker(1)

	sub0 := tr.SubscribeToCommitResult(0)
	requireResult(t, sub0.Result, nil)

	requireResult(t, tr.SubscribeToCommitResult(0).Result, nil)
	sub1 := tr.SubscribeToCommitResult(1)
	requireNoResult(t, sub1.Result)

	tr.OnFetch(2)
	require.NoError(t, tr.OnCommitIntent(2))
	require.NoError(t, tr.OnCommitIntent(1))
	require.EqualValues(t, 2, tr.PopNextToCommit())
	require.NoError(t, tr.OnCommitResult(2, commitErr))

	requireResult(t, sub1.Result, commitErr)
	sub1.unsubscribe()
	sub1 = tr.SubscribeToCommitResult(1)
	require.Error(t, tr.OnCommitResult(2, nil))
	requireNoResult(t, sub1.Result)

	sub3 := tr.SubscribeToCommitResult(3)
	sub3second := tr.SubscribeToCommitResult(3)
	sub3third := tr.SubscribeToCommitResult(3)
	sub3third.unsubscribe()
	sub4 := tr.SubscribeToCommitResult(4)
	sub5 := tr.SubscribeToCommitResult(5)
	sub6 := tr.SubscribeToCommitResult(6)
	sub20 := tr.SubscribeToCommitResult(20)

	require.Error(t, tr.OnCommitResult(1, nil))
	tr.OnFetch(3)
	tr.OnFetch(4)
	require.NoError(t, tr.OnCommitIntent(3))
	require.NoError(t, tr.OnCommitIntent(4))
	requireNoResult(t, sub1.Result)
	requireNoResult(t, sub3.Result)
	requireNoResult(t, sub3second.Result)
	require.EqualValues(t, 4, tr.PopNextToCommit())
	requireNoResult(t, sub3.Result)
	requireNoResult(t, sub3second.Result)
	require.NoError(t, tr.OnCommitResult(4, nil))
	requireResult(t, sub1.Result, nil)
	requireResult(t, sub3.Result, nil)
	requireResult(t, sub4.Result, nil)
	requireNoResult(t, sub5.Result)
	requireNoResult(t, sub20.Result)

	tr.OnFetch(10)
	requireNoResult(t, sub5.Result)
	requireNoResult(t, sub20.Result)
	sub6.unsubscribe()

	require.NoError(t, tr.OnCommitIntent(10))
	requireNoResult(t, sub5.Result)
	requireNoResult(t, sub20.Result)

	require.EqualValues(t, 10, tr.PopNextToCommit())
	require.NoError(t, tr.OnCommitResult(10, nil))
	requireResult(t, sub5.Result, nil)
	requireNoResult(t, sub6.Result)
	requireNoResult(t, sub20.Result)
	sub20.unsubscribe()
}
