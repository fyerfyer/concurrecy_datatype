package cbytes

import (
	"sync/atomic"
)

var (
	MAX_CONCURRENCY = 65536
)

type ConcurrentBytes interface {
	Concurrecy() int

	// append an element to the bytes
	Append(elem byte) (bool, error)

	// append a byte array to the bytes
	Appends(elem []byte) (int, error)

	// read the element by the index
	Get(index int) (byte, error)

	// set the element with index
	Set(index int, value byte) (bool, error)

	// get the length of the bytes
	Len() uint64
}

type concurrentBytes struct {
	concurrency int
	segments    []Segment
	total       uint64
}

func NewConcurrentBytes(concurrency int, br ByteRedistributor) (*concurrentBytes, error) {
	if concurrency <= 0 || concurrency > MAX_CONCURRENCY {
		return nil, ERROR_INVALIDCONCURRENCY
	}

	segment := make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		segment[i] = newSegment(DEFAULT_BUCKET_NUMBER, br)
	}

	return &concurrentBytes{
		concurrency: concurrency,
		segments:    segment,
		total:       uint64(0),
	}, nil
}

func (cb *concurrentBytes) Concurrecy() int {
	return cb.concurrency
}

func (cb *concurrentBytes) Append(elem byte) (bool, error) {
	s := cb.segments[int(cb.total%uint64(cb.concurrency))]
	ok, err := s.Put(elem)
	if ok {
		atomic.AddUint64(&cb.total, 1)
	}

	return ok, err
}

func (cb *concurrentBytes) Appends(elems []byte) (int, error) {
	count := 0
	for _, elem := range elems {
		s := cb.segments[int(cb.total%uint64(cb.concurrency))]
		ok, err := s.Put(elem)
		if err != nil {
			return count, err
		}
		if ok {
			atomic.AddUint64(&cb.total, 1)
			count++
		}
	}
	return count, nil
}

func (cb *concurrentBytes) Get(index int) (byte, error) {
	if index < 0 || index > int(cb.total)-1 {
		return 0, ERROR_INVALIDINDEX
	}

	s := cb.segments[int(cb.total%uint64(cb.concurrency))]
	return s.Get(index)
}

func (cb *concurrentBytes) Set(index int, value byte) (bool, error) {
	if index < 0 || index > int(cb.total)-1 {
		return false, ERROR_INVALIDINDEX
	}

	s := cb.segments[int(cb.total%uint64(cb.concurrency))]
	return s.Set(index, value)
}

func (cb *concurrentBytes) Len() uint64 {
	return cb.total
}
