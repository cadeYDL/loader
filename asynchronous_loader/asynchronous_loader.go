package level_loader

import (
	"errors"
	"sync/atomic"
	"time"
)

func WithCallback[T any](callback func(*T) error) option[T] {
	return func(f *AsynchronousLoader[T]) *AsynchronousLoader[T] {
		f.callBack = callback
		return f
	}
}

func WithLoadErrHandler[T any](loadErrorHandler func(err error)) option[T] {
	return func(f *AsynchronousLoader[T]) *AsynchronousLoader[T] {
		f.loadErrHandler = loadErrorHandler
		return f
	}
}

func WithCallbackErrHandler[T any](callbackErrorHandler func(err error)) option[T] {
	return func(f *AsynchronousLoader[T]) *AsynchronousLoader[T] {
		f.callbackErrHandler = callbackErrorHandler
		return f
	}
}

func WithStepMonitor[T any](stepMonitor func(preLoadStartAt, lastLoadStartAt, preLoadEndAt, lastLoadEndAt int64)) option[T] {
	return func(f *AsynchronousLoader[T]) *AsynchronousLoader[T] {
		f.stepMonitor = stepMonitor
		return f
	}
}

func WithEndStep[T any](endStep bool) option[T] {
	return func(f *AsynchronousLoader[T]) *AsynchronousLoader[T] {
		f.endStep = endStep
		return f
	}
}

type option[T any] func(*AsynchronousLoader[T]) *AsynchronousLoader[T]

func New[T any](stepTime time.Duration, load func() (*T, error), options ...option[T]) *AsynchronousLoader[T] {
	loader := &AsynchronousLoader[T]{
		stepTime:  stepTime,
		loader:    load,
		result:    atomic.Pointer[T]{},
		endStep:   true,
		closeChan: make(chan struct{}),
	}
	for _, op := range options {
		loader = op(loader)
	}
	return loader
}

type AsynchronousLoader[T any] struct {
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

func (f *AsynchronousLoader[T]) Close() {
	close(f.closeChan)
}

func (f *AsynchronousLoader[T]) GetResult() *T {
	return f.result.Load()
}

func (f *AsynchronousLoader[T]) check() error {
	if f == nil {
		return errors.New("cannot check asynchronous loader is nil")
	}
	if f.loader == nil {
		return errors.New("fuun loader is nil")
	}

	if f.closeChan == nil {
		f.closeChan = make(chan struct{})
	}
	return nil

}

func (f *AsynchronousLoader[T]) Do() error {
	if err := f.check(); err != nil {
		return err
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

func (f *AsynchronousLoader[T]) getNextTime() time.Duration {
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

func (f *AsynchronousLoader[T]) load() {
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
		f.lastLoadEndAt = &end
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
