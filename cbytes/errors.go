package cbytes

import "errors"

var (
	ERROR_INVALIDINDEX       = errors.New("invalid index")
	ERROR_INVALIDCONCURRENCY = errors.New("invalid concurrency input")
)
