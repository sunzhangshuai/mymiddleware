package meta

import "fmt"

// TopicPartition topic和分区
type TopicPartition struct {
	Topic     string
	Partition int32
}

func (t TopicPartition) String() string {
	return fmt.Sprintf("%s|%d", t.Topic, t.Partition)
}
