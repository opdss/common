package export

import (
	"gorm.io/gorm"
	"time"
)

// GormDataProvider Gorm查询数据迭代器
type GormDataProvider struct {
	gormDpT *GormDataProviderT[map[string]any]
}

type GormDataProviderCallback func([]map[string]any) []any

type GormDataProviderOption func(provider *GormDataProvider)

// WithGormDataProviderLimit 数据批量查询数量
func WithGormDataProviderLimit(n int) GormDataProviderOption {
	return func(provider *GormDataProvider) {
		if n > 0 {
			provider.gormDpT.limit = n
		}
	}
}

// WithGormDataProviderCallback 数据批量回调处理
func WithGormDataProviderCallback(cb GormDataProviderCallback) GormDataProviderOption {
	return func(provider *GormDataProvider) {
		if cb != nil {
			provider.gormDpT.callback = func(v []map[string]any) []any {
				return cb(v)
			}
		}
	}
}

// WithGormDataProviderQueryTimeout 单次查询超时控制
func WithGormDataProviderQueryTimeout(t time.Duration) GormDataProviderOption {
	return func(provider *GormDataProvider) {
		if t > 0 {
			provider.gormDpT.queryTimeout = t
		}
	}
}

func NewGormDataProvider(tx *gorm.DB, opts ...GormDataProviderOption) *GormDataProvider {
	g := &GormDataProvider{
		gormDpT: NewGormDataProviderT[map[string]any](tx),
	}
	for i := range opts {
		opts[i](g)
	}
	if g.gormDpT.callback == nil {
		g.gormDpT.callback = defaultCallback
	}
	return g
}

func (dp *GormDataProvider) Next() bool {
	return dp.gormDpT.Next()
}

func (dp *GormDataProvider) Value() any {
	return dp.gormDpT.Value()
}

func defaultCallback(d []map[string]any) []any {
	_data := make([]any, len(d))
	for i, v := range d {
		_data[i] = v
	}
	return _data
}
