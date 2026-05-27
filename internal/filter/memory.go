package filter

import "github.com/bits-and-blooms/bloom/v3"

type MemoryFilter struct {
	filter *bloom.BloomFilter
}

func NewMemoryFilter(size uint, percent float64) *MemoryFilter {
	filter := bloom.NewWithEstimates(size, percent)
	return &MemoryFilter{filter: filter}
}

func (f *MemoryFilter) Exists(data []byte) (bool, error) {
	return f.filter.Test(data), nil
}

func (f *MemoryFilter) Add(data []byte) error {
	_ = f.filter.Add(data)
	return nil
}

