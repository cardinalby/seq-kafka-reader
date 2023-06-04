package tracking

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOffsetSortedMap(t *testing.T) {
	t.Parallel()

	f := NewOffsetSortedMap[int](func(v int) bool {
		return v == 0
	})
	require.Equal(t, 0, f.deque.Len())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())

	f.Set(1000, 9)
	require.Equal(t, 9, f.Get(1000))
	f.Set(1000, 10)
	require.EqualValues(t, 1000, f.MinOffset())
	require.EqualValues(t, 1000, f.MaxOffset())
	require.Equal(t, 1, f.deque.Len())
	require.Equal(t, 10, f.Get(1000))
	require.Equal(t, 0, f.Get(900))
	require.Equal(t, 0, f.Get(1100))

	f.Set(1002, 12)
	require.Equal(t, 3, f.deque.Len())
	require.EqualValues(t, 1000, f.MinOffset())
	require.EqualValues(t, 1002, f.MaxOffset())
	require.Equal(t, 10, f.Get(1000))
	require.Equal(t, 12, f.Get(1002))
	require.Equal(t, 0, f.Get(1001))

	f.Set(1005, 15)
	require.Equal(t, 6, f.deque.Len())
	require.EqualValues(t, 1000, f.MinOffset())
	require.EqualValues(t, 1005, f.MaxOffset())
	require.Equal(t, 0, f.Get(1001))
	require.Equal(t, 0, f.Get(1006))
	require.Equal(t, 0, f.Get(1003))
	require.Equal(t, 0, f.Get(1004))

	f.Set(990, 0)
	require.EqualValues(t, 1000, f.MinOffset())
	require.EqualValues(t, 1005, f.MaxOffset())

	f.Set(990, 99)
	require.Equal(t, 16, f.deque.Len())
	require.EqualValues(t, 990, f.MinOffset())
	require.EqualValues(t, 1005, f.MaxOffset())

	poppedEl, ok := f.PopMin()
	require.True(t, ok)
	require.Equal(t, 99, poppedEl)
	require.Equal(t, 6, f.deque.Len())
	require.EqualValues(t, 1000, f.MinOffset())
	require.EqualValues(t, 1005, f.MaxOffset())

	f.Clear()
	require.Equal(t, 0, f.deque.Len())
	require.EqualValues(t, -1, f.MinOffset())
}

func TestOffsetSortedMap_EmptyAndPopMin(t *testing.T) {
	f := NewOffsetSortedMap[int](func(v int) bool {
		return v == 0
	})
	require.Equal(t, 0, f.deque.Len())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())

	require.EqualValues(t, 0, f.PopMinUntilSequenceEnd())
	require.Equal(t, 0, f.deque.Len())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())

	f.Set(10, 5)
	f.Set(12, 6)
	require.Equal(t, 3, f.deque.Len())
	f.Set(11, 0)
	require.Equal(t, 3, f.deque.Len())
	f.Set(11, 1)
	require.Equal(t, 3, f.deque.Len())
	require.EqualValues(t, 10, f.MinOffset())
	require.EqualValues(t, 12, f.MaxOffset())

	f.Set(11, 0)
	require.Equal(t, 3, f.deque.Len())
	f.Set(12, 0)
	require.Equal(t, 1, f.deque.Len())
	f.Set(10, 0)
	require.Equal(t, 0, f.deque.Len())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())

	el, ok := f.PopMin()
	require.False(t, ok)
	require.Equal(t, 0, el)
}

func TestOffsetSortedMap_PopUntilSequenceEnd(t *testing.T) {
	f := NewOffsetSortedMap[[]int](func(v []int) bool {
		return v == nil
	})
	f.Set(10, []int{})
	f.Set(11, []int{})
	require.Equal(t, 2, f.PopMinUntilSequenceEnd())
	require.Equal(t, 0, f.deque.Len())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())

	f.Set(10, []int{})
	f.Set(11, []int{})
	f.Set(13, []int{})
	require.Equal(t, 2, f.PopMinUntilSequenceEnd())
	require.Equal(t, 1, f.deque.Len())
	require.EqualValues(t, 13, f.MinOffset())
	require.EqualValues(t, 13, f.MaxOffset())
}

func TestOffsetSortedMap_RemoveLessOrEqual(t *testing.T) {
	f := NewOffsetSortedMap[int](func(v int) bool {
		return v == 0
	})
	f.Set(10, 100)
	f.Set(11, 110)
	f.Set(13, 130)
	f.RemoveLessOrEqual(9)
	require.Equal(t, []int64{10, 11, 13}, f.OffsetKeys())

	f.RemoveLessOrEqual(11)
	require.Equal(t, []int64{13}, f.OffsetKeys())

	f.RemoveLessOrEqual(15)
	require.Equal(t, []int64(nil), f.OffsetKeys())
	require.EqualValues(t, -1, f.MinOffset())
}

func TestOffsetSortedMap_SetEmpty(t *testing.T) {
	f := NewOffsetSortedMap[int](func(v int) bool {
		return v == 0
	})

	f.Set(10, 100)
	require.Equal(t, 1, f.deque.Len())
	f.Set(15, 150)
	require.Equal(t, 6, f.deque.Len())
	f.Set(12, 120)
	require.Equal(t, 6, f.deque.Len())
	require.EqualValues(t, 10, f.MinOffset())
	require.EqualValues(t, 15, f.MaxOffset())

	f.Set(11, 0)
	require.Equal(t, 6, f.deque.Len())
	require.EqualValues(t, 10, f.MinOffset())
	require.EqualValues(t, 15, f.MaxOffset())
	require.EqualValues(t, 0, f.Get(11))

	f.Set(10, 0)
	require.Equal(t, 4, f.deque.Len())
	require.EqualValues(t, 12, f.MinOffset())
	require.EqualValues(t, 15, f.MaxOffset())

	f.Set(15, 0)
	require.Equal(t, 1, f.deque.Len())
	require.EqualValues(t, 12, f.MinOffset())
	require.EqualValues(t, 12, f.MaxOffset())

	f.Set(12, 0)
	require.Equal(t, 0, f.deque.Len())
	require.True(t, f.IsEmpty())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())

	f.Set(20, 0)
	require.Equal(t, 0, f.deque.Len())
	require.True(t, f.IsEmpty())
	require.EqualValues(t, -1, f.MinOffset())
	require.EqualValues(t, -1, f.MaxOffset())
}

func TestOffsetSortedMap_PopMin(t *testing.T) {
	f := NewOffsetSortedMap[int](func(v int) bool {
		return v == 0
	})

	f.Set(10, 100)
	f.Set(13, 150)
	f.Set(12, 120)
	var v int
	var popped bool
	v, popped = f.PopMin()
	require.Equal(t, 100, v)
	require.True(t, popped)
	v, popped = f.PopMin()
	require.Equal(t, 120, v)
	require.True(t, popped)
	v, popped = f.PopMin()
	require.Equal(t, 150, v)
	require.True(t, popped)
	v, popped = f.PopMin()
	require.Equal(t, 0, v)
	require.False(t, popped)
}
