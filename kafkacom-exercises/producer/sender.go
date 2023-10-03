package producer

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util"
	"time"
)

type Sender struct {
	input    chan map[string]map[int32][]*meta.Message
	producer *Instance
}

// NewSender 获取新的发送器
func NewSender(producer *Instance) *Sender {
	sender := &Sender{
		input:    make(chan map[string]map[int32][]*meta.Message, 1000),
		producer: producer,
	}
	// 启动处理器
	go sender.handler()
	return sender
}

// Send 接收数据
func (s *Sender) Send(data map[string]map[int32][]*meta.Message) {
	s.input <- data
}

// handler 处理
func (s *Sender) handler() {
	for {
		func() {
			select {
			case origin := <-s.input:
				outputs := make(map[int32]*NetworkInput)
				for topicName, partitions := range origin {
					for num, messages := range partitions {
						// 实例ID
						brokerID := s.producer.Client.Metadata[topicName][num].Leader

						// 没有就创建
						if _, ok := outputs[brokerID]; !ok {
							outputs[brokerID] = &NetworkInput{
								Input:   make(map[string]map[int32][]*meta.Message),
								Request: make(map[string]map[int32]*meta.RecordBatch),
							}
						}
						if _, ok := outputs[brokerID].Input[topicName]; !ok {
							outputs[brokerID].Input[topicName] = make(map[int32][]*meta.Message)
							outputs[brokerID].Request[topicName] = make(map[int32]*meta.RecordBatch)
						}
						outputs[brokerID].Input[topicName][num] = messages
						outputs[brokerID].Request[topicName][num] = s.covert(messages)
					}
				}
				if len(outputs) > 0 {
					s.producer.Network.Send(outputs)
				}
			}
		}()
	}
}

// covert 消费信道，转成request，发消息
func (s *Sender) covert(messages []*meta.Message) *meta.RecordBatch {
	timestamp := messages[0].Timestamp
	result := &meta.RecordBatch{
		FirstOffset:           0,
		PartitionLeaderEpoch:  0,
		Version:               2,
		Codec:                 util.CompressionCodec(0),
		CompressionLevel:      -1000,
		Control:               false,
		LogAppendTime:         false,
		LastOffsetDelta:       int32(len(messages) - 1),
		FirstTimestamp:        timestamp,
		MaxTimestamp:          time.Time{},
		ProducerID:            s.producer.ProducerID,
		ProducerEpoch:         s.producer.ProducerEpoch,
		FirstSequence:         int32(messages[0].SequenceNumber),
		Records:               make([]*meta.Record, len(messages)),
		PartialTrailingRecord: false,
		IsTransactional:       false,
	}

	for i, message := range messages {
		result.Records[i] = &meta.Record{
			Headers:        nil,
			Attributes:     0,
			TimestampDelta: timestamp.Sub(result.FirstTimestamp),
			OffsetDelta:    int64(i),
			Value:          []byte(message.Value),
		}

		if message.Key != nil {
			result.Records[i].Key = []byte(*message.Key)
		}
	}
	return result
}
