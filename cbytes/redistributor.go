package cbytes

import "sync/atomic"

const (
	DEFAULT_LOAD_FACTOR     = 0.75
	DEFAULT_BUCKET_MAX_SIZE = 1000
	NORMAL                  = 1
	UNDERWEIGHT             = 2
	OVERWEIGHT              = 3
)

type ByteRedistributor interface {
	UpdateThreshold(byteTotal uint64, bucketNum int)
	CheckBucketStatus(byteTotal uint64, bucketSize uint64) int
	Redistribute(status int, buckets []Bucket) ([]Bucket, bool)
}

type byteRedistributor struct {
	loadFactor            float64
	upperThreshold        uint64
	overweightBucketCount uint64
	emptyBucketCount      uint64
}

func newByteRedistributor(loadFactor float64, bucketCount int) *byteRedistributor {
	if loadFactor <= 0 {
		loadFactor = DEFAULT_LOAD_FACTOR
	}

	br := &byteRedistributor{}
	br.loadFactor = loadFactor
	br.UpdateThreshold(0, bucketCount)
	return br
}

func (br *byteRedistributor) UpdateThreshold(byteTotal uint64, bucketNum int) {
	var average float64
	average = float64(byteTotal / uint64(bucketNum))
	if average < 100 {
		average = 100
	}

	atomic.StoreUint64(&br.upperThreshold, uint64(average*br.loadFactor))
}

func (br *byteRedistributor) CheckBucketStatus(byteTotal uint64, bucketSize uint64) int {
	if bucketSize > DEFAULT_BUCKET_MAX_SIZE ||
		bucketSize >= atomic.LoadUint64(&br.upperThreshold) {
		atomic.AddUint64(&br.overweightBucketCount, 1)
		return OVERWEIGHT
	}

	if bucketSize == 0 {
		atomic.AddUint64(&br.emptyBucketCount, 1)
		return UNDERWEIGHT
	}

	// bucket size being too small
	if bucketSize < atomic.LoadUint64(&br.upperThreshold)/4 {
		return UNDERWEIGHT
	}

	return NORMAL
}

func (br *byteRedistributor) Redistribute(status int, buckets []Bucket) ([]Bucket, bool) {
	currentSize := uint64(len(buckets))
	var newSize uint64

	switch status {
	case OVERWEIGHT:
		// if there's no so much overweighted buckets
		// there's no need to redistribute
		if atomic.LoadUint64(&br.overweightBucketCount)*4 < currentSize {
			return nil, false
		}
		newSize = currentSize << 1

	case UNDERWEIGHT:
		// less than default average number
		if currentSize < 100 ||
			atomic.LoadUint64(&br.emptyBucketCount)*4 < currentSize {
			return nil, false
		}

		newSize = currentSize >> 1
		if newSize < 2 {
			newSize = 2
		}
	default:
		return nil, false
	}

	if newSize == currentSize {
		atomic.StoreUint64(&br.overweightBucketCount, 0)
		atomic.StoreUint64(&br.emptyBucketCount, 0)
		return nil, false
	}

	var values []byte
	for _, b := range buckets {
		for i := uint64(0); i < b.Size(); i++ {
			value, err := b.Get(int(i))
			if err != nil {
				return nil, false
			}
			values = append(values, value)
		}
	}

	if newSize > currentSize {
		// if the new size is larger than the old size
		// we need to clear it one by one
		for i := uint64(0); i < currentSize; i++ {
			buckets[i].Clear(nil)
		}

		// add new bucket for each buckets
		for j := newSize - currentSize; j > 0; j-- {
			buckets = append(buckets, newBucket())
		}
	} else {
		buckets = make([]Bucket, 0)
		for j := uint64(0); j < newSize; j++ {
			buckets[j] = newBucket()
		}
	}

	// add the values to each bucket
	for _, v := range values {
		index := int(v) % int(newSize)
		buckets[index].Put(byte(v), nil)
	}

	// reset redistibuter's property
	atomic.StoreUint64(&br.overweightBucketCount, 0)
	atomic.StoreUint64(&br.emptyBucketCount, 0)

	return buckets, true
}
