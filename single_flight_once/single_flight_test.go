package single_flight_once

import (
	"sync"
	"testing"
	"time"
)

func TestSingleFlight_Do(t *testing.T) {
	count := 0
	s := BuildSingleFlight()
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			wg.Done()
			s.Do("11", func() error {
				count++
				wg.Wait()
				return nil
			})
		}()
	}
	if count != 1 {
		t.Error("count=", count)
	}

}

func TestSingleFlight_Do_Muti(t *testing.T) {
	count := 0
	s := BuildSingleFlight()
	for i := 0; i < 100; i++ {
		go func() {
			s.Do("11", func() error {
				count++
				return nil
			})
			time.Sleep(time.Second)
		}()
	}
	if count <= 1 {
		t.Error("count=", count)
	}
}

func TestSingleFlight_WithExpiration(t *testing.T) {
	wg := sync.WaitGroup{}
	count := 0
	s := BuildSingleFlight(WithExpirationAt(time.Second*105, false))
	for i := 0; i < 100; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			s.Do("11", func() error {
				count++
				return nil
			})
			time.Sleep(time.Second)
		}()
	}
	wg.Wait()
	if count != 1 {
		t.Error("count=", count)
	}

	count = 0
	s = BuildSingleFlight(WithExpirationAt(time.Second*20, true))
	for i := 0; i < 100; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			time.Sleep(time.Second)
			s.Do("11", func() error {
				count++
				time.Sleep(80 * time.Second)
				return nil
			})
		}()
	}
	wg.Wait()
	if count != 1 {
		t.Error("count=", count)
	}

	count = 0
	s = BuildSingleFlight(WithExpirationAt(time.Second*10, true))
	s.Do("11", func() error {
		count++
		return nil
	})
	time.Sleep(time.Second * 1)
	flag, _ := s.Do("11", func() error {
		count++
		return nil
	})
	if flag || count != 1 {
		t.Error("count=", count, "flag=", flag)
	}
	time.Sleep(time.Second * 9)
	flag, _ = s.Do("11", func() error {
		count++
		return nil
	})
	if !flag || count != 2 {
		t.Error("count=", count, "flag=", flag)
	}
}
