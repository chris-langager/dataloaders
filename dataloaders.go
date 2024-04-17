package dataloaders

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type DataLoader[T any] struct {
	fn LoadingFn[T]

	cache            *sync.Map
	incomingRequests chan incomingRequest[T]
}

type LoadingFn[T any] func(ctx context.Context, keys []string) ([]T, error)

func NewDataLoader[T any](fn LoadingFn[T]) *DataLoader[T] {
	dataloader := &DataLoader[T]{
		fn:               fn,
		cache:            &sync.Map{},
		incomingRequests: make(chan incomingRequest[T]),
	}
	go dataloader.loop()
	return dataloader
}

type Result[T any] struct {
	Value T
	Error error
}

var ErrorKeysMissMatch = errors.New("number of results does not match number of keys")

type incomingRequest[T any] struct {
	resultsChan chan<- Result[T]
	key         string
	ctx         context.Context
}

func (o *DataLoader[T]) loop() {
	for {
		ctx := context.Background()
		keys := make([]string, 0, 50)
		resultsChans := make([][]chan<- Result[T], 0, 50)
		receiveRequest := func(request incomingRequest[T]) {
			ctx = request.ctx
			i := findIndex(keys, request.key)
			if i == -1 {
				keys = append(keys, request.key)
				resultsChans = append(resultsChans, []chan<- Result[T]{request.resultsChan})
			} else {
				resultsChans[i] = append(resultsChans[i], request.resultsChan)
			}

		}

		request := <-o.incomingRequests
		receiveRequest(request)

		for timesUp := false; len(keys) < 50 && !timesUp; {
			select {
			case request := <-o.incomingRequests:
				receiveRequest(request)
			case <-time.After(15 * time.Millisecond):
				timesUp = true
			}
		}

		fmt.Println("calling fn...")
		fmt.Println(strings.Join(keys, ", "))
		values, err := o.fn(ctx, keys)
		if len(values) != len(keys) {
			err = ErrorKeysMissMatch
		}
		for i, cc := range resultsChans {
			for _, c := range cc {
				c <- Result[T]{
					Value: values[i],
					Error: err,
				}
			}
		}
	}
}

func (o *DataLoader[T]) Load(ctx context.Context, key string) (T, error) {
	resultsChan := make(chan Result[T], 1)
	defer close(resultsChan)

	o.incomingRequests <- incomingRequest[T]{
		ctx:         ctx,
		key:         key,
		resultsChan: resultsChan,
	}
	result := <-resultsChan

	return result.Value, result.Error
}

func (o *DataLoader[T]) LoadMany(ctx context.Context, keys []string) ([]T, error) {
	panic("not yet implemented")
}

func (o *DataLoader[T]) set(key string, value T) {
	o.cache.Store(key, value)
}

func (o *DataLoader[T]) get(key string) (T, bool) {
	a, ok := o.cache.Load(key)
	return a.(T), ok
}

func def[T any]() T {
	var t T
	return t
}

func findIndex[T comparable](elems []T, t T) int {
	for i, elem := range elems {
		if t == elem {
			return i
		}
	}
	return -1
}
