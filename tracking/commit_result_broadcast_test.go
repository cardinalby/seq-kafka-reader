package tracking

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCommitResultBroadcast(t *testing.T) {
	t.Parallel()

	b := newCommitResultBroadcast()
	err1 := errors.New("1")
	b.notify(5, err1)
	var r7, r8, r9 error
	gr := errgroup.Group{}
	sub7 := b.subscribe(7)
	sub8 := b.subscribe(8)
	sub9 := b.subscribe(9)
	require.EqualValues(t, 7, b.subs.MinOffset())
	require.EqualValues(t, 9, b.subs.MaxOffset())
	gr.Go(func() error {
		r7 = <-sub7.Result
		return nil
	})
	gr.Go(func() error {
		r8 = <-sub8.Result
		return nil
	})
	gr.Go(func() error {
		r9 = <-sub9.Result
		return nil
	})
	err2 := errors.New("2")
	b.notify(8, err2)
	err3 := errors.New("3")
	b.notify(9, err3)

	_ = gr.Wait()

	require.True(t, b.subs.IsEmpty())
	require.EqualValues(t, err2, r7)
	require.EqualValues(t, err2, r8)
	require.EqualValues(t, err3, r9)
}
