package single_flight_once

import (
	"sync"
	"time"
)

type option func(flight *SingleFlight)

func BuildSingleFlight(options ...option) *SingleFlight {
	s := &SingleFlight{}
	for _, op := range options {
		op(s)
	}
	s.record = make(map[string]*time.Time, 1)
	return s
}

func WithExpirationAt(expiration time.Duration, handlerAfter bool) option {
	return func(flight *SingleFlight) {
		flight.expiration = &expiration
		flight.handlerAfter = handlerAfter
	}
}

type SingleFlight struct {
	expiration   *time.Duration
	handlerAfter bool
	record       map[string]*time.Time
	lock         sync.Mutex
}

func (s *SingleFlight) hit(key string) bool {

	expiration, ok := s.record[key]
	if !ok {
		return false
	}
	return expiration == nil || !expiration.Before(time.Now())
}

func (s *SingleFlight) Remember(key string, ex *time.Duration) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	exist := s.hit(key)
	var expirationAt *time.Time
	if ex != nil {
		e := time.Now().Add(*s.expiration)
		expirationAt = &e
	}
	s.record[key] = expirationAt
	return exist
}

func (s *SingleFlight) Forget(key string) bool {
	s.lock.Lock()
	exist := s.hit(key)
	delete(s.record, key)
	s.lock.Unlock()
	return exist
}

func (s *SingleFlight) Do(key string, handler func() error) (bool, error) {
	s.lock.Lock()
	if s.hit(key) {
		s.lock.Unlock()
		return false, nil
	}
	if s.record == nil {
		s.record = make(map[string]*time.Time, 1)
	}
	s.record[key] = nil
	if !s.handlerAfter && s.expiration != nil {
		expiration := time.Now().Add(*s.expiration)
		s.record[key] = &expiration
	}
	s.lock.Unlock()
	err := s.do(key, handler)
	return true, err
}

func (s *SingleFlight) do(key string, handler func() error) error {
	if err := handler(); err != nil || s.expiration == nil {
		s.lock.Lock()
		delete(s.record, key)
		s.lock.Unlock()
		return err
	}

	if s.handlerAfter && s.expiration != nil {
		s.lock.Lock()
		expiration := time.Now().Add(*s.expiration)
		s.record[key] = &expiration
		s.lock.Unlock()
	}
	return nil
}
