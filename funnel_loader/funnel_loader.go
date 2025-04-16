package funnel_loader

import (
	"context"
	"fmt"
	"sort"

	"golang.org/x/sync/singleflight"
)

func BuildCacheLoader[val any](cacheLoader func(ctx context.Context, keys []string) (map[string]val, error), cacheSetter func(ctx context.Context, result map[string]val) error) *cacheDefaultLoader[val] {
	if cacheLoader == nil {
		return nil
	}
	if cacheSetter == nil {
		return nil
	}
	return &cacheDefaultLoader[val]{
		cacheLoader: cacheLoader,
		cacheSetter: cacheSetter,
	}
}

var _ DataSource[int] = &cacheDefaultLoader[int]{}

type cacheDefaultLoader[VAL any] struct {
	cacheLoader func(ctx context.Context, keys []string) (map[string]VAL, error)
	cacheSetter func(ctx context.Context, result map[string]VAL) error
	keyFormat   string
}

func (s *cacheDefaultLoader[VAL]) MGet(ctx context.Context, keys []string) (result map[string]VAL, miss []string, err map[string]error) {
	res, innerErr := s.cacheLoader(ctx, keys)
	if innerErr != nil {
		errMap := make(map[string]error, len(keys))
		for _, key := range keys {
			errMap[key] = innerErr
		}
		return nil, keys, errMap
	}
	result = make(map[string]VAL, len(keys))
	miss = make([]string, 0, len(keys))
	for _, key := range keys {
		v, ok := res[key]
		if !ok {
			miss = append(miss, key)
			continue
		}
		result[key] = v
	}
	return result, miss, nil
}

func (s *cacheDefaultLoader[VAL]) MSet(ctx context.Context, result map[string]VAL) map[string]error {
	if err := s.cacheSetter(ctx, result); err != nil {
		errMap := make(map[string]error, len(result))
		for key := range result {
			errMap[key] = err
		}
		return errMap
	}
	return nil
}

type levelLoaderErrorStatus string

const (
	levelLoaderErrorStatusMGet levelLoaderErrorStatus = "mget"
	levelLoaderErrorStatusMSet levelLoaderErrorStatus = "mset"
)

type DataSource[V any] interface {
	MGet(ctx context.Context, keys []string) (result map[string]V, miss []string, err map[string]error)
	MSet(ctx context.Context, result map[string]V) map[string]error
}

// BuildFunnelDataLoader  创建数据源加载流程的模版,将接口按照加载顺序写入，比如localCache->remoteCache->db,就写入BuildLevelDataLoader(localCache,remoteCache,db)
func BuildFunnelDataLoader[V any](title string, sources ...DataSource[V]) *funnelDataLoader[V] {
	loader := &funnelDataLoader[V]{
		dataSourceList: sources,
		title:          title,
		sf:             singleflight.Group{},
	}
	return loader
}

type funnelDataLoader[V any] struct {
	dataSourceList []DataSource[V]
	title          string

	sf singleflight.Group
}

func (l *funnelDataLoader[V]) GetLoaderLen() int {
	return len(l.dataSourceList)
}

func (l *funnelDataLoader[V]) buildLoader() func(ctx context.Context, keys []string, useSFInx int, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (result map[string]V, miss []string, getError map[string]map[int]error, setError map[string]map[int]error) {
	var invoke func(ctx context.Context, keys []string, useSFInx int, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (map[string]V, []string, map[string]map[int]error, map[string]map[int]error)
	for inx := len(l.dataSourceList) - 1; inx >= 0; inx-- {
		inx := inx
		source := l.dataSourceList[inx]
		curInvoke := invoke

		invoke = func(ctx context.Context, keys []string, useSFInx int, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (map[string]V, []string, map[string]map[int]error, map[string]map[int]error) {
			var (
				getError map[string]error
				setError map[string]error
			)
			loadFunc := func(ctx context.Context, keys []string, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (result map[string]V, miss []string, getErrors map[string]map[int]error, setErrors map[string]map[int]error) {
				result = make(map[string]V, len(keys))
				if _, ok := skipGetInx[inx]; !ok && len(keys) > 0 {
					result, miss, getError = source.MGet(ctx, keys)
					keys = miss
				}
				if curInvoke != nil && len(keys) > 0 {
					nextResult, nextMiss, nextGetErr, nextSetErr := curInvoke(ctx, keys, useSFInx, skipGetInx, skipSetInx)
					getErrors = nextGetErr
					setErrors = nextSetErr
					if _, ok := skipSetInx[inx]; len(nextResult) > 0 && !ok {
						setError = source.MSet(ctx, nextResult)
					}
					for key, val := range nextResult {
						result[key] = val
					}
					miss = nextMiss
				}
				getErrors = l.mergeErrors(getErrors, inx, getError)
				setErrors = l.mergeErrors(setErrors, inx, setError)
				return
			}
			if useSFInx != inx {
				return loadFunc(ctx, keys, skipGetInx, skipSetInx)
			}
			return l.buildLoaderWithSF(loadFunc)(ctx, keys, skipGetInx, skipSetInx)
		}
	}
	return invoke
}

func (l *funnelDataLoader[V]) buildLoaderWithSF(invoke func(ctx context.Context, keys []string, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (result map[string]V, miss []string, getError map[string]map[int]error, setError map[string]map[int]error)) func(ctx context.Context, keys []string, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (result map[string]V, miss []string, getError map[string]map[int]error, setError map[string]map[int]error) {
	return func(ctx context.Context, keys []string, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}) (result map[string]V, miss []string, getError map[string]map[int]error, setError map[string]map[int]error) {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		type res struct {
			result   map[string]V
			miss     []string
			getError map[string]map[int]error
			setError map[string]map[int]error
		}
		r, _, _ := l.sf.Do(l.title+fmt.Sprintf("%v", keys), func() (interface{}, error) {
			hit, missKeys, getErr, setErr := invoke(ctx, keys, skipGetInx, skipSetInx)
			return &res{
				result:   hit,
				miss:     missKeys,
				getError: getErr,
				setError: setErr,
			}, nil
		})
		if r == nil {
			return
		}
		rStruct := r.(*res)
		return rStruct.result, rStruct.miss, rStruct.getError, rStruct.setError
	}
}

func (l *funnelDataLoader[V]) mergeErrors(resErr map[string]map[int]error, inx int, errs ...map[string]error) map[string]map[int]error {
	if resErr == nil {
		resErr = make(map[string]map[int]error)
	}
	for _, err := range errs {
		for key, e := range err {
			if resErr[key] == nil {
				resErr[key] = make(map[int]error, 1)
			}
			resErr[key][inx] = e
		}
	}
	return resErr
}

func (l *funnelDataLoader[V]) Load(ctx context.Context, keys []string, skipGetInx map[int]struct{}, skipSetInx map[int]struct{}, useSFInx *int) (result map[string]V, miss []string, getErrors, setErrors map[string]map[int]error) {
	if l == nil || len(l.dataSourceList) == 0 {
		return nil, keys, nil, nil
	}
	if skipGetInx == nil {
		skipGetInx = make(map[int]struct{})
	}
	if skipSetInx == nil {
		skipSetInx = make(map[int]struct{})
	}
	sfInx := -1
	if useSFInx != nil {
		sfInx = *useSFInx
	}
	return l.buildLoader()(ctx, keys, sfInx, skipGetInx, skipSetInx)
}
