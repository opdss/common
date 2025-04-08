package export

// SliceDataProvider 数组数据迭代器
type SliceDataProvider struct {
	index int
	size  int
	data  []any
}

func NewSliceDataProvider(data []any) *SliceDataProvider {
	return &SliceDataProvider{
		data:  data,
		index: 0,
		size:  len(data),
	}
}

func (dp *SliceDataProvider) Next() bool {
	if dp.index < dp.size {
		return true
	}
	return false
}

func (dp *SliceDataProvider) Value() any {
	defer func() {
		dp.index++
	}()
	if dp.index < dp.size {
		return dp.data[dp.index]
	}
	return nil
}
