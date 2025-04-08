package export

import (
	"context"
	"time"
)

type FlowDataProviderFn func(ctx context.Context, lastId, lastTs int64, limit int) ([]FlowDataProviderRecord, error)

type FlowDataProviderCallback func([]FlowDataProviderRecord) []any

type FlowDataProviderRecord interface {
	GetId() int64
	GetTs() int64
}

type FlowDataProviderOption func(provider *FlowDataProvider)

// WithFlowDataProviderLimit 数据批量查询数量
func WithFlowDataProviderLimit(n int) FlowDataProviderOption {
	return func(provider *FlowDataProvider) {
		if n > 0 {
			provider.limit = n
		}
	}
}

// WithFlowDataProviderQueryTimeout 单次查询超时控制
func WithFlowDataProviderQueryTimeout(t time.Duration) FlowDataProviderOption {
	return func(provider *FlowDataProvider) {
		if t > 0 {
			provider.queryTimeout = t
		}
	}
}

// WithFlowDataProviderOptionCallback 数据批量回调处理
func WithFlowDataProviderOptionCallback(cb FlowDataProviderCallback) FlowDataProviderOption {
	return func(provider *FlowDataProvider) {
		if cb != nil {
			provider.callback = cb
		}
	}
}

// FlowDataProvider Gorm查询数据迭代器
type FlowDataProvider struct {
	lastId       int64
	lastTs       int64
	limit        int
	hasMore      bool
	queryTimeout time.Duration
	callback     FlowDataProviderCallback
	sliceDp      *SliceDataProvider
	queryFn      FlowDataProviderFn
}

// NewFlowDataProvider 瀑布流式获取记录流水，数据获取一定是按照主键的顺序
func NewFlowDataProvider(queryFn FlowDataProviderFn, opts ...FlowDataProviderOption) *FlowDataProvider {
	g := &FlowDataProvider{
		lastId:       0,
		lastTs:       0,
		limit:        2000,
		hasMore:      true,
		queryTimeout: time.Second * 30,
		callback:     nil,
		sliceDp:      NewSliceDataProvider([]any{}),
		queryFn:      queryFn,
	}

	for i := range opts {
		opts[i](g)
	}
	return g
}

func (dp *FlowDataProvider) Next() bool {
	if !dp.hasMore {
		return false
	}
	hasNext := dp.sliceDp.Next()
	if hasNext {
		return hasNext
	}
	ctx, cancel := context.WithTimeout(context.Background(), dp.queryTimeout)
	defer cancel()
	list, err := dp.queryFn(ctx, dp.lastId, dp.lastTs, dp.limit)
	if err != nil {
		dp.hasMore = false
		return false
	}
	if len(list) == 0 {
		dp.hasMore = false
		return false
	}
	dp.sliceDp = NewSliceDataProvider(dp.defaultCallbackT(list))
	return dp.sliceDp.Next()
}

func (dp *FlowDataProvider) Value() any {
	return dp.sliceDp.Value()
}

func (dp *FlowDataProvider) defaultCallbackT(d []FlowDataProviderRecord) []any {
	if dp.callback == nil {
		_data := make([]any, len(d))
		for i, v := range d {
			dp.lastId = v.GetId()
			dp.lastTs = v.GetTs()
			_data[i] = v
		}
		return _data
	} else {
		for i := range d {
			dp.lastId = d[i].GetId()
			dp.lastTs = d[i].GetTs()
		}
		return dp.callback(d)
	}
}
