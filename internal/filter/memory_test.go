package filter

import (
	"strconv"
	"sync"
	"testing"
)

func TestMemoryFilterConcurrentAccess(t *testing.T) {
	const workers, perWorker = 4, 256
	f := NewMemoryFilter(workers*perWorker+1, 0.01)
	known := []byte("known")
	if err := f.Add(known); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for i := range perWorker {
				key := strconv.Itoa(worker*perWorker + i)
				if err := f.Add([]byte(key)); err != nil {
					t.Errorf("Add(%q): %v", key, err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			for range perWorker {
				exists, err := f.Exists(known)
				if err != nil || !exists {
					t.Errorf("Exists(%q) = %v, %v; want true, nil", known, exists, err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	for i := range workers * perWorker {
		key := strconv.Itoa(i)
		exists, err := f.Exists([]byte(key))
		if err != nil || !exists {
			t.Fatalf("Exists(%q) = %v, %v after concurrent writes; want true, nil", key, exists, err)
		}
	}
}
