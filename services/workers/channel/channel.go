package channel

import "time"

type Job struct {
	Id      int
	Method  string
	URL     string
	Headers map[string]string
	Body    []byte
}

type Result struct {
	JobId        int
	StatusCode   int
	Duration     time.Duration
	BytesFetched int64
	Err          error
}
