package cbytes

import (
	"sync"
	"sync/atomic"
)

var (
	DEFAULT_BUCKET_NUMBER = 16
)

type Bucket interface {
	Put(elem byte, lock sync.Locker) (bool, error)
	Get(index int) (byte, error)
	Set(index int, value byte) (bool, error)
	Delete(index int, lock sync.Locker) bool
	Clear(lock sync.Locker)
	Size() uint64
	String() string
}

type bucket struct {
	values []byte
	size   uint64
}

func newBucket() *bucket {
	return &bucket{
		values: make([]byte, 0),
	}
}

func (b *bucket) Put(elem byte, lock sync.Locker) (bool, error) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	b.values = append(b.values, elem)
	atomic.AddUint64(&b.size, 1)
	return true, nil
}

func (b *bucket) Get(index int) (byte, error) {
	if index > int(b.size)-1 || index < 0 {
		return 0, ERROR_INVALIDINDEX
	}

	return b.values[index], nil
}

func (b *bucket) Set(index int, value byte) (bool, error) {
	if int(b.size)-1 < index || index < 0 {
		return false, ERROR_INVALIDINDEX
	}

	b.values[index] = value
	return true, nil
}

func (b *bucket) Delete(index int, lock sync.Locker) bool {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	if index < 0 || index > int(b.size) {
		return false
	}

	b.values = append(b.values[:index], b.values[index+1:]...)
	atomic.AddUint64(&b.size, ^uint64(0))
	return true
}

func (b *bucket) Clear(lock sync.Locker) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	b.values = make([]byte, 0)
	atomic.StoreUint64(&b.size, 0)
}

func (b *bucket) Size() uint64 {
	return atomic.LoadUint64(&b.size)
}

func (b *bucket) String() string {
	return string(b.values)
}
