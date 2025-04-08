package export

import (
	"context"
	"gorm.io/gorm"
	"time"
)

// GormDataProviderT Gorm查询数据迭代器
type GormDataProviderT[T any] struct {
	tx           *gorm.DB
	offset       int
	limit        int
	hasMore      bool
	findMode     bool
	queryTimeout time.Duration
	callback     GormDataProviderTCallback[T]
	sliceDp      *SliceDataProvider
}

type GormDataProviderTCallback[T any] func([]T) []any

type GormDataProviderTOption[T any] func(provider *GormDataProviderT[T])

// WithGormDataProviderTLimit 数据批量查询数量
func WithGormDataProviderTLimit[T any](n int) GormDataProviderTOption[T] {
	return func(provider *GormDataProviderT[T]) {
		if n > 0 {
			provider.limit = n
		}
	}
}

// WithGormDataProviderTCallback 数据批量回调处理
func WithGormDataProviderTCallback[T any](cb GormDataProviderTCallback[T]) GormDataProviderTOption[T] {
	return func(provider *GormDataProviderT[T]) {
		if cb != nil {
			provider.callback = cb
		}
	}
}

// WithGormDataProviderTQueryTimeout 单次查询超时控制
func WithGormDataProviderTQueryTimeout[T any](t time.Duration) GormDataProviderTOption[T] {
	return func(provider *GormDataProviderT[T]) {
		if t > 0 {
			provider.queryTimeout = t
		}
	}
}

// WithGormDataProviderTModel 使用find查询
func WithGormDataProviderTModel[T any](findMode bool) GormDataProviderTOption[T] {
	return func(provider *GormDataProviderT[T]) {
		provider.findMode = findMode
	}
}

// NewGormDataProviderT 范型的gorm的dp实现，注意 T 不能是指针
func NewGormDataProviderT[T any](tx *gorm.DB, opts ...GormDataProviderTOption[T]) *GormDataProviderT[T] {
	g := &GormDataProviderT[T]{
		tx:           tx,
		offset:       0,
		limit:        2000,
		hasMore:      true,
		queryTimeout: time.Second * 30,
		callback:     defaultCallbackT[T],
		sliceDp:      NewSliceDataProvider([]any{}),
	}
	for i := range opts {
		opts[i](g)
	}
	return g
}

func (dp *GormDataProviderT[T]) Next() bool {
	if !dp.hasMore {
		return false
	}
	hasNext := dp.sliceDp.Next()
	if hasNext {
		return hasNext
	}
	res := make([]T, 0)
	ctx, cancel := context.WithTimeout(context.Background(), dp.queryTimeout)
	defer cancel()
	var err error
	if dp.findMode {
		err = dp.tx.WithContext(ctx).Offset(dp.offset).Limit(dp.limit).Find(&res).Error
	} else {
		err = dp.tx.WithContext(ctx).Offset(dp.offset).Limit(dp.limit).Scan(&res).Error
	}
	if err != nil {
		dp.hasMore = false
		return false
	}
	if len(res) == 0 {
		dp.hasMore = false
		return false
	}
	dp.offset += dp.limit
	dp.sliceDp = NewSliceDataProvider(dp.callback(res))
	return dp.sliceDp.Next()
}

func (dp *GormDataProviderT[T]) Value() any {
	return dp.sliceDp.Value()
}

func defaultCallbackT[T any](d []T) []any {
	_data := make([]any, len(d))
	for i, v := range d {
		_data[i] = v
	}
	return _data
}
