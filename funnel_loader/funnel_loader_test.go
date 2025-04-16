package funnel_loader

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _ DataSource[string] = &dataSourceImpl{}

type dataSourceImpl struct {
	database   map[string]string
	title      string
	err        map[string]error
	queryCount int
	setOne     int

	lock sync.RWMutex
}

func (a *dataSourceImpl) MGet(ctx context.Context, keys []string) (result map[string]string, miss []string, err map[string]error) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	a.queryCount += len(keys)
	result = make(map[string]string)
	miss = make([]string, 0, len(keys))
	for _, k := range keys {
		if v, ok := a.database[k]; ok {
			result[k] = v
		} else {
			miss = append(miss, k)
		}
	}
	fmt.Println(fmt.Sprintf(a.title+"get keys:%v", keys))
	err = a.err
	return
}

func (a *dataSourceImpl) MSet(ctx context.Context, result map[string]string) map[string]error {
	a.lock.Lock()
	defer a.lock.Unlock()
	for k, v := range result {
		a.database[k] = v
	}
	fmt.Println(fmt.Sprintf(a.title+"set keys:%v", result))
	return a.err
}

type dataSourcePImpl struct {
	database            map[string]string
	title               string
	err                 map[string]error
	queryCount          int
	readOnce, writeOnce int
}

func (a *dataSourcePImpl) MGet(ctx context.Context, keys []string) (result map[string]string, miss []string, err map[string]error) {
	time.Sleep(3 * time.Second)
	a.readOnce++
	a.queryCount += len(keys)
	result = make(map[string]string)
	miss = make([]string, 0, len(keys))
	for _, k := range keys {
		if v, ok := a.database[k]; ok {
			result[k] = v
		} else {
			miss = append(miss, k)
		}
	}
	fmt.Println(fmt.Sprintf(a.title+"get keys:%v", keys))
	err = a.err
	return
}

func (a *dataSourcePImpl) MSet(ctx context.Context, result map[string]string) map[string]error {
	a.writeOnce++
	for k, v := range result {
		a.database[k] = v
	}
	fmt.Println(fmt.Sprintf(a.title+"set keys:%v", result))
	return a.err
}

func Test_levelDataLoader_LoadWithSF(t *testing.T) {
	a := &dataSourceImpl{
		database: map[string]string{"a": "a"},
		title:    "a",
	}
	b := &dataSourcePImpl{
		database: map[string]string{"b": "b"},
		title:    "b",
	}
	c := &dataSourceImpl{
		database: map[string]string{"c": "c", "b": "c"},
		title:    "c",
	}
	ctx := context.Background()
	loader := BuildFunnelDataLoader[string]("ZYY_TEST", a, b, c)
	pWait := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		pWait.Add(1)
		go func() {
			defer pWait.Done()
			v := 1
			hit, miss, _, _ := loader.Load(ctx, []string{"a", "b", "c", "d", "e"}, nil, nil, &v)
			assert.Equal(t, hit["a"], "a")
			assert.Equal(t, hit["b"], "b")
			assert.Equal(t, hit["c"], "c")
			assert.Equal(t, len(miss), 2)
		}()
	}
	pWait.Wait()
	assert.Equal(t, b.readOnce, 1)
	assert.Equal(t, b.writeOnce, 1)
	t.Log(1)
}

func Test_levelDataLoader_Load(t *testing.T) {
	a := &dataSourceImpl{
		database: map[string]string{"a": "a"},
		title:    "a",
	}
	b := &dataSourceImpl{
		database: map[string]string{"b": "b", "a": "b"},
		title:    "b",
	}
	c := &dataSourceImpl{
		database: map[string]string{"c": "c", "b": "c", "a": "c", "d": "c"},
		title:    "c",
	}
	d := &dataSourceImpl{
		database: map[string]string{},
		title:    "d",
	}
	ctx := context.Background()
	loader := BuildFunnelDataLoader[string]("ZYY_TEST", a, b, d, c)

	r1, m1, _, _ := loader.Load(ctx, []string{"a", "b", "c"}, nil, map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}}, nil)
	assert.Equal(t, len(r1), 3)
	assert.Equal(t, len(m1), 0)
	assert.Equal(t, r1["a"], "a")
	assert.Equal(t, r1["b"], "b")
	assert.Equal(t, r1["c"], "c")
	assert.Equal(t, a.queryCount, 3)
	assert.Equal(t, b.queryCount, 2)
	assert.Equal(t, d.queryCount, 1)
	assert.Equal(t, c.queryCount, 1)
	assert.Equal(t, len(c.database), 4)
	assert.Equal(t, len(b.database), 2)
	assert.Equal(t, len(a.database), 1)
	assert.Equal(t, len(d.database), 0)
	a.queryCount = 0
	b.queryCount = 0
	c.queryCount = 0
	d.queryCount = 0

	r2, m2, _, _ := loader.Load(ctx, []string{"a", "b", "c"}, map[int]struct{}{0: {}}, map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}}, nil)
	assert.Equal(t, len(r2), 3)
	assert.Equal(t, len(m2), 0)
	assert.Equal(t, r2["a"], "b")
	assert.Equal(t, r2["b"], "b")
	assert.Equal(t, r2["c"], "c")
	assert.Equal(t, a.queryCount, 0)
	assert.Equal(t, b.queryCount, 3)
	assert.Equal(t, d.queryCount, 1)
	assert.Equal(t, c.queryCount, 1)
	assert.Equal(t, len(c.database), 4)
	assert.Equal(t, len(b.database), 2)
	assert.Equal(t, len(a.database), 1)
	assert.Equal(t, len(d.database), 0)
	a.queryCount = 0
	b.queryCount = 0
	c.queryCount = 0
	d.queryCount = 0

	r3, m3, _, _ := loader.Load(ctx, []string{"a", "b", "c"}, map[int]struct{}{0: {}, 1: {}}, map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}}, nil)
	assert.Equal(t, len(r3), 3)
	assert.Equal(t, len(m3), 0)
	assert.Equal(t, r3["a"], "c")
	assert.Equal(t, r3["b"], "c")
	assert.Equal(t, r3["c"], "c")
	assert.Equal(t, a.queryCount, 0)
	assert.Equal(t, b.queryCount, 0)
	assert.Equal(t, d.queryCount, 3)
	assert.Equal(t, c.queryCount, 3)
	assert.Equal(t, len(c.database), 4)
	assert.Equal(t, len(b.database), 2)
	assert.Equal(t, len(a.database), 1)
	assert.Equal(t, len(d.database), 0)
	a.queryCount = 0
	b.queryCount = 0
	c.queryCount = 0
	d.queryCount = 0

	r4, m4, _, _ := loader.Load(ctx, []string{"a", "b", "c", "d"}, nil, nil, nil)
	assert.Equal(t, len(r4), 4)
	assert.Equal(t, len(m4), 0)
	assert.Equal(t, r4["a"], "a")
	assert.Equal(t, r4["b"], "b")
	assert.Equal(t, r4["c"], "c")
	assert.Equal(t, r4["d"], "c")
	assert.Equal(t, a.database["a"], "a")
	assert.Equal(t, a.database["b"], "b")
	assert.Equal(t, a.database["c"], "c")
	assert.Equal(t, a.database["d"], "c")
	assert.Equal(t, b.database["b"], "b")
	assert.Equal(t, b.database["c"], "c")
	assert.Equal(t, b.database["d"], "c")
	assert.Equal(t, c.database["d"], "c")
	assert.Equal(t, c.database["c"], "c")
	assert.Equal(t, d.database["d"], "c")
	assert.Equal(t, d.database["c"], "c")
	assert.Equal(t, len(c.database), 4)
	assert.Equal(t, len(b.database), 4)
	assert.Equal(t, len(a.database), 4)
	assert.Equal(t, len(d.database), 2)
	assert.Equal(t, a.queryCount, 4)
	assert.Equal(t, b.queryCount, 3)
	assert.Equal(t, d.queryCount, 2)
	assert.Equal(t, c.queryCount, 2)
	a.queryCount = 0
	b.queryCount = 0
	c.queryCount = 0
	d.queryCount = 0

	r5, m5, _, _ := loader.Load(ctx, []string{"a", "b", "c", "d", "e"}, nil, nil, nil)
	assert.Equal(t, len(r5), 4)
	assert.Equal(t, len(m5), 1)
	assert.Equal(t, r5["a"], "a")
	assert.Equal(t, r5["b"], "b")
	assert.Equal(t, r5["c"], "c")
	assert.Equal(t, r5["d"], "c")
	assert.Equal(t, a.database["a"], "a")
	assert.Equal(t, a.database["b"], "b")
	assert.Equal(t, a.database["c"], "c")
	assert.Equal(t, a.database["d"], "c")
	assert.Equal(t, b.database["b"], "b")
	assert.Equal(t, b.database["c"], "c")
	assert.Equal(t, b.database["d"], "c")
	assert.Equal(t, c.database["d"], "c")
	assert.Equal(t, c.database["c"], "c")
	assert.Equal(t, d.database["d"], "c")
	assert.Equal(t, d.database["c"], "c")
	assert.Equal(t, len(c.database), 4)
	assert.Equal(t, len(b.database), 4)
	assert.Equal(t, len(a.database), 4)
	assert.Equal(t, len(d.database), 2)
	assert.Equal(t, a.queryCount, 5)
	assert.Equal(t, b.queryCount, 1)
	assert.Equal(t, d.queryCount, 1)
	assert.Equal(t, c.queryCount, 1)
	a.queryCount = 0
	b.queryCount = 0
	c.queryCount = 0
	d.queryCount = 0
}

func TestCacheLoader(t *testing.T) {
	v := map[string]int32{
		"a": 1,
	}
	loaderFunc1 := func(ctx context.Context, keys []string) (map[string]int32, error) {
		return v, nil
	}
	setFunc1 := func(ctx context.Context, result map[string]int32) error {
		for key, val := range result {
			v[key] = val
		}
		return nil
	}
	loader := BuildCacheLoader[int32](loaderFunc1, setFunc1)
	ctx := context.Background()
	hit, miss, _ := loader.MGet(ctx, []string{"a", "b", "c", "d", "e"})
	assert.Equal(t, hit["a"], int32(1))
	assert.Equal(t, len(miss), int(4))
	loader.MSet(ctx, map[string]int32{
		"b": 2,
		"c": 3,
		"a": 4,
	})
	hit, miss, _ = loader.MGet(ctx, []string{"a", "b", "c", "d", "e"})
	assert.Equal(t, hit["a"], int32(4))
	assert.Equal(t, hit["b"], int32(2))
	assert.Equal(t, hit["c"], int32(3))
	assert.Equal(t, len(miss), int(2))
}
