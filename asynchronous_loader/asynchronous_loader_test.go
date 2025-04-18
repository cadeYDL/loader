package level_loader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetNextTime(t *testing.T) {
	val := 0
	al := New[int](10*time.Second, func() (*int, error) {
		val++
		time.Sleep(4 * time.Second)
		return &val, nil
	}, WithEndStep[int](true))
	next := al.getNextTime()
	assert.Equal(t, next, time.Duration(0))
	al.load()
	next = al.getNextTime()
	assert.Equal(t, next, 10*time.Second)

	alf := New[int](10*time.Second, func() (*int, error) {
		val++
		time.Sleep(4 * time.Second)
		return &val, nil
	}, WithEndStep[int](false))
	nextF := alf.getNextTime()
	assert.Equal(t, nextF, time.Duration(0))
	alf.load()
	nextF = alf.getNextTime()
	assert.GreaterOrEqual(t, nextF, 5*time.Second)
	assert.LessOrEqual(t, nextF, 6*time.Second)
}

func TestLoad(t *testing.T) {
	val := 0
	al := New[int](10*time.Second, func() (*int, error) {
		val++
		return &val, nil
	}, WithEndStep[int](false))
	al.load()
	al.load()
	assert.Equal(t, val, int(2))

	res := al.GetResult()
	assert.Equal(t, *res, int(2))
}

func TestDo(t *testing.T) {
	val := make([]int, 0, 1)
	al := New[[]int](1*time.Second, func() (*[]int, error) {
		val = append(val, 1)
		return &val, nil
	}, WithEndStep[[]int](false))
	err := al.Do()
	assert.NoError(t, err)
	time.Sleep(10 * time.Second)
	assert.Equal(t, len(val), 10)
}

func TestCheck(t *testing.T) {
	var al *AsynchronousLoader[[]int]

	err := al.check()
	assert.Equal(t, err != nil, true)

	al = &AsynchronousLoader[[]int]{}
	err = al.check()
	assert.Equal(t, err != nil, true)
	val := []int{1, 2, 3}
	al = &AsynchronousLoader[[]int]{
		loader: func() (*[]int, error) {
			return &val, nil
		},
	}
	err = al.check()
	assert.Equal(t, err != nil, false)
}

func TestClose(t *testing.T) {
	val := make([]int, 0, 1)
	al := New[[]int](1*time.Second, func() (*[]int, error) {
		val = append(val, 1)
		return &val, nil
	}, WithEndStep[[]int](false))
	al.Do()
	time.Sleep(10 * time.Second)
	al.Close()
	time.Sleep(10 * time.Second)
	assert.Less(t, len(val), 11)
}

func TestCallback(t *testing.T) {

}

func TestErrHandler(t *testing.T) {

}

func TestCallbackHandler(t *testing.T) {

}

func TestStepMonitor(t *testing.T) {

}
