package iterator

import (
	"context"
	"github.com/opdss/common/contracts/iterator"
	"time"
)

var _ iterator.Iterator[any] = (*FlowQueryIterator[any])(nil)

type FlowQueryIteratorFn[T any] func(ctx context.Context, lastModel T, limit int) ([]T, error)

type FlowQueryIteratorOption[T any] func(provider *FlowQueryIterator[T])

// WithFlowQueryIteratorLimit 数据批量查询数量
func WithFlowQueryIteratorLimit[T any](n int) FlowQueryIteratorOption[T] {
	return func(provider *FlowQueryIterator[T]) {
		if n > 0 {
			provider.limit = n
		}
	}
}

// WithFlowQueryIteratorQueryTimeout 单次查询超时控制
func WithFlowQueryIteratorQueryTimeout[T any](t time.Duration) FlowQueryIteratorOption[T] {
	return func(provider *FlowQueryIterator[T]) {
		if t > 0 {
			provider.queryTimeout = t
		}
	}
}

// FlowQueryIterator Gorm查询数据迭代器
type FlowQueryIterator[T any] struct {
	lastModel    T
	limit        int
	hasMore      bool
	queryTimeout time.Duration
	sliceIter    *SliceIterator[T]
	queryFn      FlowQueryIteratorFn[T]
}

// NewFlowQueryIterator 瀑布流式获取记录流水，数据获取一定是按照主键的顺序
func NewFlowQueryIterator[T any](queryFn FlowQueryIteratorFn[T], opts ...FlowQueryIteratorOption[T]) *FlowQueryIterator[T] {
	g := &FlowQueryIterator[T]{
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

func (dp *FlowQueryIterator[T]) Next() bool {
	if !dp.hasMore {
		return false
	}
	hasNext := dp.sliceIter.Next()
	if hasNext {
		return hasNext
	}
	ctx, cancel := context.WithTimeout(context.Background(), dp.queryTimeout)
	defer cancel()
	list, err := dp.queryFn(ctx, dp.lastModel, dp.limit)
	if err != nil {
		dp.hasMore = false
		return false
	}
	if len(list) == 0 {
		dp.hasMore = false
		return false
	}
	dp.sliceIter = NewSliceIterator(list)
	return dp.sliceIter.Next()
}

func (dp *FlowQueryIterator[T]) Value() T {
	dp.lastModel = dp.sliceIter.Value()
	return dp.lastModel
}
