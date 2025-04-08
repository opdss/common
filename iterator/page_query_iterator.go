package iterator

import (
	"context"
	"github.com/opdss/common/contracts/iterator"
	"time"
)

var _ iterator.Iterator[any] = (*PageQueryIterator[any])(nil)

type PageQueryIteratorFn[T any] func(ctx context.Context, offset, limit int) ([]T, error)

type PageQueryIteratorOption[T any] func(provider *PageQueryIterator[T])

// WithPageQueryIteratorLimit 数据批量查询数量
func WithPageQueryIteratorLimit[T any](n int) PageQueryIteratorOption[T] {
	return func(provider *PageQueryIterator[T]) {
		if n > 0 {
			provider.limit = n
		}
	}
}

// WithPageQueryIteratorQueryTimeout 单次查询超时控制
func WithPageQueryIteratorQueryTimeout[T any](t time.Duration) PageQueryIteratorOption[T] {
	return func(provider *PageQueryIterator[T]) {
		if t > 0 {
			provider.queryTimeout = t
		}
	}
}

// PageQueryIterator Gorm查询数据迭代器
type PageQueryIterator[T any] struct {
	offset       int
	limit        int
	hasMore      bool
	queryTimeout time.Duration
	sliceIter    *SliceIterator[T]
	queryFn      PageQueryIteratorFn[T]
}

// NewPageQueryIterator 瀑布流式获取记录流水，数据获取一定是按照主键的顺序
func NewPageQueryIterator[T any](queryFn PageQueryIteratorFn[T], opts ...PageQueryIteratorOption[T]) *PageQueryIterator[T] {
	g := &PageQueryIterator[T]{
		offset:       0,
		limit:        2000,
		hasMore:      true,
		queryTimeout: time.Second * 30,
		sliceIter:    NewSliceIterator(make([]T, 0)),
		queryFn:      queryFn,
	}

	for i := range opts {
		opts[i](g)
	}
	return g
}

func (dp *PageQueryIterator[T]) Next() bool {
	if !dp.hasMore {
		return false
	}
	hasNext := dp.sliceIter.Next()
	if hasNext {
		return hasNext
	}
	ctx, cancel := context.WithTimeout(context.Background(), dp.queryTimeout)
	defer cancel()
	list, err := dp.queryFn(ctx, dp.offset, dp.limit)
	if err != nil {
		dp.hasMore = false
		return false
	}
	if len(list) == 0 {
		dp.hasMore = false
		return false
	}
	dp.offset += dp.limit
	dp.sliceIter = NewSliceIterator(list)
	return dp.sliceIter.Next()
}

func (dp *PageQueryIterator[T]) Value() T {
	return dp.sliceIter.Value()
}
