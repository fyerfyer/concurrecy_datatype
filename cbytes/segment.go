package cbytes

import (
	"sync"
	"sync/atomic"
)

type Segment interface {
	Put(elem byte) (bool, error)
	Get(index int) (byte, error)
	Set(index int, value byte) (bool, error)
	Delete(index int) bool
	Size() uint64
}

type segment struct {
	buckets           []Bucket
	bucketsLen        int
	byteTotal         uint64
	byteRedistributor ByteRedistributor
	lock              sync.Mutex
}

func newSegment(bucketNum int, byteRedistributor ByteRedistributor) Segment {
	if bucketNum <= 0 {
		bucketNum = DEFAULT_BUCKET_NUMBER
	}
	if byteRedistributor == nil {
		byteRedistributor = newByteRedistributor(DEFAULT_LOAD_FACTOR, bucketNum)
	}

	// init the buckets
	buckets := make([]Bucket, 0)
	for i := 0; i < bucketNum; i++ {
		buckets[i] = newBucket()
	}

	return &segment{
		buckets:           buckets,
		bucketsLen:        bucketNum,
		byteRedistributor: byteRedistributor,
	}
}

func (s *segment) Put(elem byte) (bool, error) {
	s.lock.Lock()
	b := s.buckets[int(s.byteTotal%uint64(s.bucketsLen))]
	ok, err := b.Put(elem, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.byteTotal, 1)
		s.redistribute(newTotal, b.Size())
	}

	s.lock.Unlock()
	return ok, err
}

func (s *segment) Get(index int) (byte, error) {
	s.lock.Lock()
	b := s.buckets[index%s.bucketsLen]
	s.lock.Unlock()
	return b.Get(index)
}

func (s *segment) Delete(index int) bool {
	s.lock.Lock()
	b := s.buckets[index%s.bucketsLen]
	ok := b.Delete(index, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.byteTotal, ^uint64(0))
		s.redistribute(newTotal, b.Size())
	}

	s.lock.Unlock()
	return ok
}

func (s *segment) Set(index int, value byte) (bool, error) {
	s.lock.Lock()
	b := s.buckets[index%s.bucketsLen]
	ok, err := b.Set(index, value)
	s.lock.Unlock()
	return ok, err
}

func (s *segment) Size() uint64 {
	return atomic.LoadUint64(&s.byteTotal)
}

func (s *segment) redistribute(bytetotal uint64, bucketSize uint64) error {
	defer func() {
		if err := recover(); err != nil {
			panic(err)
		}
	}()

	s.byteRedistributor.UpdateThreshold(bytetotal, s.bucketsLen)
	status := s.byteRedistributor.CheckBucketStatus(bytetotal, bucketSize)
	newBuckets, ok := s.byteRedistributor.Redistribute(status, s.buckets)
	if ok {
		s.buckets = newBuckets
		s.bucketsLen = len(s.buckets)
	}

	return nil
}
