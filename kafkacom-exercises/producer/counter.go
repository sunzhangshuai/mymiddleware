package producer

import "fmt"

// Counter 消息的计数器
type Counter struct {
	data map[string]int64
}

// NewCounter 消息的计数器
func NewCounter() *Counter {
	return &Counter{data: make(map[string]int64)}
}

func (c *Counter) Get(topicName string, partitionNum int32) int64 {
	key := fmt.Sprintf("%s-%d", topicName, partitionNum)

	if _, ok := c.data[key]; !ok {
		c.data[key] = 0
	}

	res := c.data[key]
	c.data[key]++
	return res
}
