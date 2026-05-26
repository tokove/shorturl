package sequence

// Sequence 定义接口，方便用其他类型实现，比如Redis实现、MySQL实现等
type Sequence interface {
	Next() (uint64, error)
}
