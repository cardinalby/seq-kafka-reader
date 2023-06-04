package tracking

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"cardinalby/seq-kafka-reader/types"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestMessagesCommitResultSubs_Aggregate(t *testing.T) {
	t.Parallel()
	r := make(MessageResultSubs, 100)
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		i := i
		ch := make(chan error, 1)
		wg.Add(1)
		r[&kafka.Message{Offset: int64(i)}] = &MsgResultSubscription{Result: ch}
		go func() {
			time.Sleep(time.Duration(i % 4))
			if i%2 == 0 {
				ch <- errors.New(strconv.Itoa(i))
			} else {
				ch <- nil
			}
			close(ch)
			wg.Done()
		}()
	}
	var res types.MessageErrors
	wg.Add(1)
	go func() {
		res = <-r.Aggregate(context.TODO())
		wg.Done()
	}()
	wg.Wait()

	require.NotNil(t, res)
	for _, me := range res {
		numberFromErr, err := strconv.Atoi(me.Err.Error())
		require.EqualValues(t, me.Message.Offset%2, 0)
		require.NoError(t, err)
		require.EqualValues(t, me.Message.Offset, numberFromErr)
	}
}

func TestMessagesCommitResultSubs_ContextCancellation(t *testing.T) {
	t.Parallel()
	r := make(MessageResultSubs, 100)
	msg1 := &kafka.Message{Offset: 1}
	msg2 := &kafka.Message{Offset: 2}
	ch1 := make(chan error, 1)
	r[msg1] = &MsgResultSubscription{Result: ch1}
	r[msg2] = &MsgResultSubscription{Result: make(chan error, 1)}

	ctx, cancel := context.WithCancel(context.TODO())
	resChan := r.Aggregate(ctx)
	ch1 <- nil
	time.Sleep(time.Millisecond * 10)
	cancel()
	res := <-resChan
	require.Len(t, res, 1)
	require.Equal(t, context.Canceled, res[0].Err)
	msgErr := res[0]
	require.True(t, errors.Is(msgErr, context.Canceled))
}
