package filter

import (
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type MemoryFilter struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
}

func NewMemoryFilter(size uint, percent float64) *MemoryFilter {
	filter := bloom.NewWithEstimates(size, percent)
	return &MemoryFilter{filter: filter}
}

func (f *MemoryFilter) Exists(data []byte) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.filter.Test(data), nil
}

func (f *MemoryFilter) Add(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.filter.Add(data)
	return nil
}
