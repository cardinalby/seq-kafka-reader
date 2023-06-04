package tracking

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const committingGoroutines = 3

const partitionsNumber = 10
const offsetsNumber = int64(10000)

const flushIntervalMin = time.Millisecond * 10 // flushing ready offsets interval
const flushIntervalMax = time.Millisecond * 20

const waitBeforeCommitIntentMin = time.Microsecond * 100 // how long consumer processes a message
const waitBeforeCommitIntentMax = time.Millisecond * 100

const commitDurationMin = time.Millisecond * 1 // commit operation duration
const commitDurationMax = time.Millisecond * 50

const waitBeforeNextFetchMin = time.Microsecond // how long to wait until new message is available
const waitBeforeNextFetchMax = time.Microsecond * 150

const commitErrorProbability = 0.3

type rng struct {
	rand *rand.Rand
	mu   sync.Mutex
}

func (r *rng) duration(min, max time.Duration) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return time.Duration(r.rand.Int63n(int64(max-min+1))) + min
}

func (r *rng) getCommitResult(offset int64) error {
	if offset == offsetsNumber {
		return nil // the last commit should succeed so that test can finish
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if 1-r.rand.Float32() <= commitErrorProbability {
		return commitErr
	}
	return nil
}

var random = rng{rand: rand.New(rand.NewSource(time.Now().UnixNano()))}

func Test_TopicCommitTracker_Concurrent_TickerCommit(t *testing.T) {
	tr := NewTopicCommitTracker()

	committingErrGroup := &errgroup.Group{}
	partitionsDone := 0

	for i := 0; i < committingGoroutines; i++ {
		committingErrGroup.Go(func() error {
			ticker := time.NewTicker(1)
			defer ticker.Stop()

			for {
				<-ticker.C
				ticker.Reset(random.duration(flushIntervalMin, flushIntervalMax))
				toCommit := tr.PopNextToCommit()
				for i := range toCommit {
					msg := &toCommit[i]
					committingErrGroup.Go(func() error {
						time.Sleep(random.duration(commitDurationMin, commitDurationMax))
						err := tr.OnCommitResult(msg, random.getCommitResult(msg.Offset))
						require.NoError(t, err)
						return err
					})
					if msg.Offset == offsetsNumber {
						partitionsDone++
					}
				}
				if partitionsDone == partitionsNumber {
					return nil
				}
			}
		})
	}

	startTime := time.Now()

	producingErrGroup := &errgroup.Group{}
	for partition := 0; partition < partitionsNumber; partition++ {
		partition := partition
		producingErrGroup.Go(func() error {
			for offset := int64(1); offset <= offsetsNumber; offset++ {
				msg := &kafka.Message{Partition: partition, Offset: offset}
				tr.OnFetch(msg) // send fetch to tracker
				producingErrGroup.Go(func() error {
					time.Sleep(random.duration(waitBeforeCommitIntentMin, waitBeforeCommitIntentMax))
					err := tr.OnCommitIntent([]kafka.Message{*msg})
					assert.Nil(t, err) // send commit to tracker
					return err
				})
				time.Sleep(random.duration(waitBeforeNextFetchMin, waitBeforeNextFetchMax))
			}
			return nil
		})
	}
	_ = producingErrGroup.Wait()
	fmt.Printf("producing+intents finished in: %d ms\n", time.Now().Sub(startTime).Milliseconds())

	_ = committingErrGroup.Wait()
	fmt.Printf("comitting finished in: %d ms\n", time.Now().Sub(startTime).Milliseconds())
}
