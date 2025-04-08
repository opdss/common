package iterator

import "github.com/opdss/common/contracts/iterator"

var _ iterator.Iterator[any] = (*SliceIterator[any])(nil)

// SliceIterator 数组数据迭代器
type SliceIterator[T any] struct {
	index int
	size  int
	data  []T
}

func NewSliceIterator[T any](data []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		data:  data,
		index: 0,
		size:  len(data),
	}
}

func (dp *SliceIterator[T]) Next() bool {
	if dp.index < dp.size {
		return true
	}
	return false
}

func (dp *SliceIterator[T]) Value() T {
	defer func() {
		dp.index++
	}()
	if dp.index < dp.size {
		return dp.data[dp.index]
	}
	var v T
	return v
}
