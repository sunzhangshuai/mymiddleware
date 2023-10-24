package producer

import (
	"fmt"
	"sync"
)

// Counter 消息的计数器
type Counter struct {
	data map[string]int64
	sync.Mutex
}

// NewCounter 消息的计数器
func NewCounter() *Counter {
	return &Counter{data: make(map[string]int64)}
}

func (c *Counter) Get(topicName string, partitionNum int32) int64 {
	c.Lock()
	defer c.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionNum)

	if _, ok := c.data[key]; !ok {
		c.data[key] = 0
	}

	res := c.data[key]
	c.data[key]++
	return res
}
