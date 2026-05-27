package filter

// Filter 定义了布隆过滤器的接口，支持内存版本和Redis版本
type Filter interface {
	Exists(data []byte) (bool, error)
	Add(data []byte) error
}
