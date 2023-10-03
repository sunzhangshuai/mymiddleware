package meta

import (
	"time"
)

// MessageResponse 结果
type MessageResponse struct {
	Partition int32
	Offset    int64
	Err       error
}

// Message 消息结果
type Message struct {
	Key          *string
	Value        string
	Topic        string
	PartitionNum int32
	Timestamp    time.Time

	Callback func(*MessageResponse)
	// 解决同步问题
	SyncSign chan *MessageResponse

	SequenceNumber int64
}
