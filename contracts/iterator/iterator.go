package iterator

type Iterator[T any] interface {
	//Next 是否有下一条数据
	Next() bool
	//Value 获取下一条数据
	Value() T
}
