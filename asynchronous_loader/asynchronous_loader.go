package level_loader

import (
	"errors"
	"sync/atomic"
	"time"
)

func WithCallback[T any](callback func(*T) error) option[T] {
	return func(f *FuunelLoader[T]) {
		f.callBack = callback
	}
}

func WithLoadErrHandler[T any](loadErrorHandler func(err error)) option[T] {
	return func(f *FuunelLoader[T]) {
		f.loadErrHandler = loadErrorHandler
	}
}

func WithCallbackErrHandler[T any](callbackErrorHandler func(err error)) option[T] {
	return func(f *FuunelLoader[T]) {
		f.callbackErrHandler = callbackErrorHandler
	}
}

func WithStepMonitor[T any](stepMonitor func(preLoadStartAt, lastLoadStartAt, preLoadEndAt, lastLoadEndAt int64)) option[T] {
	return func(f *FuunelLoader[T]) {
		f.stepMonitor = stepMonitor
	}
}

type option[T any] func(*FuunelLoader[T])

func New[T any](stepTime time.Duration, load func() (*T, error), options ...option[T]) *FuunelLoader[T] {
	loader := &FuunelLoader[T]{
		stepTime:    stepTime,
		loader:      load,
		result:      atomic.Pointer[T]{},
		endStep:     true,
		stepMonitor: nil,
		closeChan:   make(chan struct{}),
	}
	for _, op := range options {
		op(loader)
	}
	return loader
}

type FuunelLoader[T any] struct {
	stepTime                           time.Duration
	callBack                           func(t *T) error
	loader                             func() (*T, error)
	result                             atomic.Pointer[T]
	endStep                            bool
	lastLoadStartAt, lastLoadEndAt     *time.Time
	loadErrHandler, callbackErrHandler func(err error)
	stepMonitor                        func(preLoadStartAt, lastLoadStartAt, preLoadEndAt, lastLoadEndAt int64)
	closeChan                          chan struct{}
}

func (f *FuunelLoader[T]) Close() {
	close(f.closeChan)
}

func (f *FuunelLoader[T]) GetResult() *T {
	return f.result.Load()
}

func (f *FuunelLoader[T]) Load() error {
	if f.loader == nil {
		return errors.New("fuun loader is nil")
	}

	go func() {
		for {
			select {
			case <-f.closeChan:
				return
			case <-time.After(f.getNextTime()):
				f.load()
			}
		}
	}()
	return nil
}

func (f *FuunelLoader[T]) getNextTime() time.Duration {
	if f.lastLoadEndAt == nil {
		return 0
	}
	if f.endStep {
		return f.stepTime
	}
	step := f.lastLoadStartAt.Add(f.stepTime).Sub(time.Now())
	for step < 0 {
		step += f.stepTime
	}
	return step
}

func (f *FuunelLoader[T]) load() {
	start := time.Now()
	defer func() {
		f.lastLoadStartAt = &start
		if f.lastLoadEndAt.Before(start) {
			f.lastLoadEndAt = &start
		}
	}()
	res, err := f.loader()
	if err != nil && f.loadErrHandler != nil {
		f.loadErrHandler(err)
	}

	if f.callBack != nil {
		err = f.callBack(res)
		if err != nil && f.callbackErrHandler != nil {
			f.callbackErrHandler(err)
		}
	}
	f.result.Store(res)
	end := time.Now()
	defer func() {
		f.lastLoadStartAt = &end
	}()
	if f.stepMonitor != nil {
		preStart := int64(0)
		if f.lastLoadStartAt != nil {
			preStart = f.lastLoadStartAt.Unix()
		}
		preEnd := int64(0)
		if f.lastLoadEndAt != nil {
			preEnd = f.lastLoadEndAt.Unix()
		}
		f.stepMonitor(preStart, start.Unix(), preEnd, end.Unix())
	}
	return
}
