package seqreader

import (
	"context"
	"sync"
	"time"

	"cardinalby/seq-kafka-reader/tracking"
	"cardinalby/seq-kafka-reader/types"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	*kafka.Reader
	commitInterval    time.Duration
	internalCtx       context.Context
	internalCtxCancel context.CancelFunc
	waitGroupCancel   context.CancelFunc
	commitTracker     *tracking.TopicCommitTracker
	fetchMu           sync.Mutex
	readyToCommit     sync.Cond
}

func NewReader(config kafka.ReaderConfig) *Reader {
	commitInterval := config.CommitInterval
	// make underlying kafka-go reader always commit synchronously
	config.CommitInterval = 0

	reader := &Reader{
		Reader:         kafka.NewReader(config),
		commitInterval: commitInterval,
		commitTracker:  tracking.NewTopicCommitTracker(),
		fetchMu:        sync.Mutex{},
	}
	reader.internalCtx, reader.internalCtxCancel = context.WithCancel(context.Background())
	if commitInterval > 0 {
		go func() {
			reader.runCommitLoop(reader.internalCtx)
		}()
	}
	return reader
}

func (r *Reader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.fetchMu.Lock()
	defer r.fetchMu.Unlock()
	msg, err := r.Reader.FetchMessage(ctx)
	if err != nil {
		return msg, err
	}
	r.commitTracker.OnFetch(&msg)
	return msg, nil
}

func (r *Reader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	var commitResSub tracking.MessageResultSubs
	if r.useSyncCommits() {
		// subscribe first to make sure we get the results anyway
		commitResSub = r.commitTracker.SubscribeToCommitResult(msgs)
	}
	if err := r.commitTracker.OnCommitIntent(msgs); err != nil {
		// it shouldn't happen if correct msgs are passed
		commitResSub.Unsubscribe()
		return err
	}

	if r.useSyncCommits() {
		return <-commitResSub.Aggregate(ctx)
	}
	return nil
}

func (r *Reader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	// copy the origin ReadMessage logic
	m, err := r.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, err
	}

	if r.Reader.Config().GroupID != "" {
		if err := r.CommitMessages(ctx, m); err != nil {
			return kafka.Message{}, err
		}
	}

	return m, nil
}

func (r *Reader) Close() error {
	r.internalCtxCancel()
	return r.Reader.Close()
}

func (r *Reader) runCommitLoop(ctx context.Context) {
	ticker := time.NewTicker(r.commitInterval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			if messages := r.commitTracker.PopNextToCommit(); len(messages) > 0 {
				msgErrors := r.commit(ctx, messages)
				r.withErrorLogger(func(logger kafka.Logger) {
					logger.Printf("commit error: %s", msgErrors.Error())
				})
			}
		}
	}
}

func (r *Reader) commit(ctx context.Context, messages []kafka.Message) types.MessageErrors {
	var messagesError types.MessageErrors
	for i := range messages {
		msg := &messages[i]
		if err := r.Reader.CommitMessages(ctx, *msg); err != nil {
			messagesError = append(messagesError, types.MessageError{Message: msg, Err: err})
			continue
		}
		if err := r.commitTracker.OnCommitResult(msg, nil); err != nil {
			r.withErrorLogger(func(logger kafka.Logger) {
				logger.Printf("error tracking commit success: %s", err.Error())
			})
		}
	}
	return messagesError
}

func (r *Reader) useSyncCommits() bool {
	return r.commitInterval == 0
}

func (r *Reader) withErrorLogger(do func(kafka.Logger)) {
	if r.Reader.Config().ErrorLogger != nil {
		do(r.Reader.Config().ErrorLogger)
	} else if r.Reader.Config().Logger != nil {
		do(r.Reader.Config().Logger)
	}
}
