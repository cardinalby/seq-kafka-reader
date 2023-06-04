package tracking

import (
	"context"
	"sync"

	"cardinalby/seq-kafka-reader/types"
	"github.com/segmentio/kafka-go"
)

type MessageResultSubs map[*kafka.Message]*MsgResultSubscription

// Unsubscribe from all underlying subscriptions
// should be called before Aggregate
func (subs MessageResultSubs) Unsubscribe() {
	for _, sub := range subs {
		sub.unsubscribe()
	}
}

func (subs MessageResultSubs) Aggregate(ctx context.Context) <-chan types.MessageErrors {
	var messagesError types.MessageErrors
	messagesErrorMu := sync.Mutex{}

	wg := sync.WaitGroup{}
	for msgPtr, sub := range subs {
		msgPtr := msgPtr
		sub := sub
		wg.Add(1)
		go func() {
			var err error
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case err = <-sub.Result:
			}
			if err != nil { // success of sub was unsubscribed and the channel closed
				messagesErrorMu.Lock()
				messagesError = append(messagesError, types.MessageError{Message: msgPtr, Err: err})
				messagesErrorMu.Unlock()
			}
			wg.Done()
		}()
	}

	resChan := make(chan types.MessageErrors, 1)
	go func() {
		wg.Wait()
		resChan <- messagesError
		close(resChan)
	}()

	return resChan
}
