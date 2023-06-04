package tracking

import (
	"github.com/gammazero/deque"
)

// OffsetSortedMap is wrapper around deque.Deque[T] to record values associated with int64 offsets
// It uses a circular buffer under the hood and doesn't allocate memory when you add higher offsets while
// remove old offsets and a Result range max-min doesn't change a lot. It grows or shrinks if needed.
// Memory consumption depends only on max-min value, not on absolute values of offsets
// Use it when gaps between offsets eventually get filled
type OffsetSortedMap[T any] struct {
	min        int64
	deque      *deque.Deque[T]
	emptyValue T
	isEmpty    func(T) bool
}

func NewOffsetFlags() *OffsetSortedMap[bool] {
	return NewOffsetSortedMap[bool](func(v bool) bool { return !v })
}

func NewOffsetSortedMap[T any](isEmpty func(T) bool) *OffsetSortedMap[T] {
	return &OffsetSortedMap[T]{
		deque:   deque.New[T](),
		min:     -1,
		isEmpty: isEmpty,
	}
}

// Set sets value for the offset
func (of *OffsetSortedMap[T]) Set(o int64, value T) {
	if of.isEmpty(value) {
		of.setEmptyValue(o)
		return
	}

	if of.deque.Len() == 0 {
		of.min = o
		of.deque.PushBack(value)
		return
	}

	if o == of.min {
		of.deque.Set(0, value)
		return
	}

	if o < of.min {
		gapSize := int(of.min - o - 1)
		of.pushElementsFront(gapSize, of.emptyValue)
		of.deque.PushFront(value)
		of.min = o
		return
	}

	// o > of.min
	targetIndex := of.getTargetIndex(o)
	if targetIndex < of.deque.Len() { // targetIndex is inside gap
		of.deque.Set(targetIndex, value)
		return
	}

	// outside of Len, grow
	gapSize := targetIndex - of.deque.Len()
	of.pushElementsBack(gapSize, of.emptyValue)
	of.deque.PushBack(value)
}

// Get returns a value for offset o if set or default value of type T
func (of *OffsetSortedMap[T]) Get(o int64) T {
	if of.deque.Len() == 0 || o < of.min {
		return of.emptyValue
	}

	if targetIndex := of.getTargetIndex(o); targetIndex < of.deque.Len() {
		return of.deque.At(targetIndex)
	}
	return of.emptyValue
}

// Clear clears underlying deque retaining its current capacity to avoid GC during reuse and resets
// min tracked offset to -1
func (of *OffsetSortedMap[T]) Clear() {
	of.deque.Clear()
	of.min = -1
}

// PopMin pops minimal offset set with not empty value and returns (value, true) if the container wasn't empty
// or (T default value, false) if it was empty
func (of *OffsetSortedMap[T]) PopMin() (T, bool) {
	if of.IsEmpty() {
		return of.emptyValue, false
	}
	item := of.deque.PopFront() // front element is always not empty

	emptyPopped := of.popEmptyFrontElements()
	if of.IsEmpty() {
		of.min = -1
	} else {
		of.min += int64(1 + emptyPopped) // 1 is popped item
	}

	return item, true
}

// PopMinUntilSequenceEnd pops items from the front while they constitute a sequence of not empty values
func (of *OffsetSortedMap[T]) PopMinUntilSequenceEnd() (poppedNumber int) {
	dequeLen := of.deque.Len()
	for ; poppedNumber < dequeLen && !of.isEmpty(of.deque.Front()); poppedNumber++ { // until front element is set
		of.deque.PopFront() // remove
	}

	// remove unset elements in the gap before next set
	emptyPopped := of.popEmptyFrontElements()
	if of.IsEmpty() {
		of.min = -1
	} else {
		of.min += int64(poppedNumber + emptyPopped)
	}
	return poppedNumber
}

// RemoveLessOrEqual removes all offsets <= 0
func (of *OffsetSortedMap[T]) RemoveLessOrEqual(o int64) {
	popCount := of.getTargetIndex(o) + 1
	if popCount >= of.deque.Len() {
		popCount = of.deque.Len()
	}
	of.popElementsFront(popCount)
	emptyPopped := of.popEmptyFrontElements()
	if of.IsEmpty() {
		of.min = -1
	} else {
		of.min += int64(popCount + emptyPopped)
	}
}

// IsEmpty returns true if any offsets set with not empty values
func (of *OffsetSortedMap[T]) IsEmpty() bool {
	return of.deque.Len() == 0
}

// MinOffset returns the min offset with not empty value if any or -1
func (of *OffsetSortedMap[T]) MinOffset() int64 {
	return of.min
}

// MaxOffset returns the max offset with not empty value if any or -1
func (of *OffsetSortedMap[T]) MaxOffset() int64 {
	dequeLen := of.deque.Len()
	if dequeLen > 0 {
		return of.min + int64(dequeLen-1)
	}
	return -1
}

// OffsetKeys is used for debug/tests purposes. Returns array of all offsets with not empty values
func (of *OffsetSortedMap[T]) OffsetKeys() []int64 {
	var res []int64
	for i := 0; i < of.deque.Len(); i++ {
		if !of.isEmpty(of.deque.At(i)) {
			res = append(res, of.min+int64(i))
		}
	}
	return res
}

func (of *OffsetSortedMap[T]) setEmptyValue(o int64) {
	if of.isEmpty(of.Get(o)) { // deque is empty or offset is not set with not empty value
		return
	}
	targetIndex := of.getTargetIndex(o)
	if targetIndex == 0 {
		of.deque.PopFront()
		emptyPopped := of.popEmptyFrontElements()
		if of.deque.Len() == 0 {
			of.min = -1
		} else {
			of.min += int64(emptyPopped + 1)
		}
		return
	}
	if targetIndex == of.deque.Len()-1 {
		of.deque.PopBack()
		of.popEmptyBackElements()
		// of.deque.Len() can't be == 0, because it would mean that targetIndex was the only element
		// in the deque, but this case is handled by "if targetIndex == 0" branch.
		// Also, of.min is not needed to be adjusted
		return
	}
	of.deque.Set(targetIndex, of.emptyValue)
}

func (of *OffsetSortedMap[T]) getTargetIndex(o int64) int {
	return int(o - of.min)
}

// popElementsFront(false) can be implemented with O(1) with custom deque impl
func (of *OffsetSortedMap[T]) popElementsFront(count int) {
	for i := 0; i < count; i++ {
		of.deque.PopFront()
	}
}

func (of *OffsetSortedMap[T]) pushElementsFront(count int, val T) {
	for i := 0; i < count; i++ {
		of.deque.PushFront(val)
	}
}

// pushElementsBack(false) can be implemented with O(1) with custom deque impl
func (of *OffsetSortedMap[T]) pushElementsBack(count int, val T) {
	for i := 0; i < count; i++ {
		of.deque.PushBack(val)
	}
}

// removeFrontElements can be implemented with O(1) with custom deque impl
func (of *OffsetSortedMap[T]) popEmptyFrontElements() int {
	i := 0
	for ; of.deque.Len() > 0 && of.isEmpty(of.deque.Front()); i++ { // go forward and find next not set element
		of.deque.PopFront()
	}
	return i
}

// removeFrontElements can be implemented with O(1) with custom deque impl
func (of *OffsetSortedMap[T]) popEmptyBackElements() {
	for of.deque.Len() > 0 && of.isEmpty(of.deque.Back()) { // go backward and find next not set element
		of.deque.PopBack()
	}
}
