package dataloaders

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataloader_LoadBasic(t *testing.T) {
	ctx := context.Background()
	spy := NewSpy(func(ctx context.Context, keys []string) ([]string, error) {
		return keys, nil
	})
	dataloader := NewDataLoader(spy.Load)

	data, err := dataloader.Load(ctx, "A")
	assert.Equal(t, 1, len(spy.CalledWith))
	assert.ElementsMatch(t, []string{"A"}, spy.CalledWith[0])
	assert.NoError(t, err)
	assert.Equal(t, "A", data)
}

func TestDataloader_LoadConcurrent(t *testing.T) {
	ctx := context.Background()
	spy := NewSpy(func(ctx context.Context, keys []string) ([]string, error) {
		return keys, nil
	})
	dataloader := NewDataLoader(spy.Load)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			dataloader.Load(ctx, strconv.Itoa(i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, len(spy.CalledWith))
	assert.ElementsMatch(t, []string{"0", "1", "2"}, spy.CalledWith[0])
}

func TestDataloader_LoadDuplicates(t *testing.T) {
	ctx := context.Background()
	spy := NewSpy(func(ctx context.Context, keys []string) ([]string, error) {
		return keys, nil
	})
	dataloader := NewDataLoader(spy.Load)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			dataloader.Load(ctx, "1")
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, len(spy.CalledWith))
	assert.ElementsMatch(t, []string{"1"}, spy.CalledWith[0])
}

type Spy[T any] struct {
	CalledWith [][]string //track function calls and the keys arg
	fn         LoadingFn[T]
}

func NewSpy[T any](fn LoadingFn[T]) *Spy[T] {
	return &Spy[T]{
		fn: fn,
	}
}

func (o *Spy[T]) Load(ctx context.Context, keys []string) ([]T, error) {
	o.CalledWith = append(o.CalledWith, keys)
	return o.fn(ctx, keys)
}
