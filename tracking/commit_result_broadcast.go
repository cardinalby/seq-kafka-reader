package tracking

import (
	"container/heap"
	"sync"
)

type MsgResultSubscription struct {
	Result    chan error
	offset    int64
	broadcast *CommitResultBroadcast
}

func CreateNewSuccessMsgResultSubscription() *MsgResultSubscription {
	chanWithSuccess := make(chan error, 1)
	chanWithSuccess <- nil
	close(chanWithSuccess)
	return &MsgResultSubscription{Result: chanWithSuccess}
}

func (s *MsgResultSubscription) unsubscribe() {
	if s.broadcast != nil {
		s.broadcast.unsubscribe(s)
	}
}

type CommitResultBroadcast struct {
	s    heap.Interface
	subs *OffsetSortedMap[[]chan error]
	mu   sync.Mutex
}

func newCommitResultBroadcast() *CommitResultBroadcast {
	return &CommitResultBroadcast{
		subs: NewOffsetSortedMap(func(v []chan error) bool {
			return len(v) == 0
		}),
	}
}

func (b *CommitResultBroadcast) unsubscribe(sub *MsgResultSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subChannels := b.subs.Get(sub.offset)
	for i, subChan := range subChannels {
		if subChan == sub.Result {
			// replace it with the last (since the order is not important), cleaning the last
			// if i == lastIndex, it will make just one extra assignment
			lastIndex := len(subChannels) - 1
			subChannels[i] = subChannels[lastIndex]
			subChannels[lastIndex] = nil
			subChannels = subChannels[:lastIndex]

			close(sub.Result)
			// setting empty subChannels with len == 0 effectively deletes the offset
			b.subs.Set(sub.offset, subChannels)
			break
		}
	}
	sub.broadcast = nil
}

func (b *CommitResultBroadcast) subscribe(offset int64) *MsgResultSubscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan error, 1)
	b.subs.Set(offset, append(b.subs.Get(offset), ch))
	return &MsgResultSubscription{
		Result:    ch,
		offset:    offset,
		broadcast: b,
	}
}

func (b *CommitResultBroadcast) notify(offset int64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subs.IsEmpty() {
		return
	}

	for !b.subs.IsEmpty() && b.subs.MinOffset() <= offset {
		subChannels, _ := b.subs.PopMin()
		for _, subCh := range subChannels {
			subCh <- err
			close(subCh)
		}
	}
}
