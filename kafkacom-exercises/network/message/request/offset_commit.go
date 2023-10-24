package request

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// ReceiveTime is a special value for the Timestamp field of Offset Commit Requests which
// tells the broker to set the Timestamp to the time at which the request was received.
// The Timestamp is only used if message version 1 is used, which requires kafka 0.8.2.
const ReceiveTime int64 = -1

// GroupGenerationUndefined is a special value for the group generation field of
// Offset Commit Requests that should be used when a consumer group does not rely
// on Kafka for partition management.
const GroupGenerationUndefined = -1

// offsetCommitRequestBlock 提交块
type offsetCommitRequestBlock struct {
	Offset    int64
	Timestamp int64
	Metadata  string
}

func (b *offsetCommitRequestBlock) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt64(b.Offset)
	pe.PutInt64(b.Timestamp)
	return pe.PutString(b.Metadata)
}

func (b *offsetCommitRequestBlock) Decode(pd decoder.PacketDecoder) error {
	var err error

	if b.Offset, err = pd.GetInt64(); err != nil {
		return err
	}
	if b.Timestamp, err = pd.GetInt64(); err != nil {
		return err
	}
	b.Metadata, err = pd.GetString()
	return err
}

// OffsetCommitRequest 偏移量提交请求
type OffsetCommitRequest struct {
	ConsumerGroup           string // 组ID
	ConsumerGroupGeneration int32  // 会话ID
	ConsumerID              string // 消费者ID

	Blocks map[string]map[int32]*offsetCommitRequestBlock
}

func (r *OffsetCommitRequest) Encode(pe encoder.PacketEncoder) error {
	if err := pe.PutString(r.ConsumerGroup); err != nil {
		return err
	}
	pe.PutInt32(r.ConsumerGroupGeneration)
	if err := pe.PutString(r.ConsumerID); err != nil {
		return err
	}
	if err := pe.PutArrayLength(len(r.Blocks)); err != nil {
		return err
	}
	for topic, partitions := range r.Blocks {
		if err := pe.PutString(topic); err != nil {
			return err
		}
		if err := pe.PutArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.PutInt32(partition)
			if err := block.Encode(pe); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetCommitRequest) Decode(pd decoder.PacketDecoder) error {
	var err error
	if r.ConsumerGroup, err = pd.GetString(); err != nil {
		return err
	}
	if r.ConsumerGroupGeneration, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.ConsumerID, err = pd.GetString(); err != nil {
		return err
	}
	topicCount, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	r.Blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.GetString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.GetArrayLength()
		if err != nil {
			return err
		}
		r.Blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.GetInt32()
			if err != nil {
				return err
			}
			block := &offsetCommitRequestBlock{}
			if err = block.Decode(pd); err != nil {
				return err
			}
			r.Blocks[topic][partition] = block
		}
	}
	return nil
}

func (r *OffsetCommitRequest) APIKey() int16 {
	return 8
}

func (r *OffsetCommitRequest) APIVersion() int16 {
	return 6
}

func (r *OffsetCommitRequest) AddBlockWithLeaderEpoch(topic string, partitionID int32, offset int64, metadata string) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	}

	if r.Blocks[topic] == nil {
		r.Blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
	}

	r.Blocks[topic][partitionID] = &offsetCommitRequestBlock{offset, ReceiveTime, metadata}
}
