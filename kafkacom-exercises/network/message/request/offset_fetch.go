package request

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type OffsetFetchRequest struct {
	ConsumerGroup string             // 组ID
	Partitions    map[string][]int32 // 分区列表
}

func (r *OffsetFetchRequest) Encode(pe encoder.PacketEncoder) error {
	var err error
	if err = pe.PutString(r.ConsumerGroup); err != nil {
		return err
	}
	if err = pe.PutArrayLength(len(r.Partitions)); err != nil {
		return err
	}
	for topic, partitions := range r.Partitions {
		if err = pe.PutString(topic); err != nil {
			return err
		}
		if err = pe.PutInt32Array(partitions); err != nil {
			return err
		}
	}
	return nil
}

func (r *OffsetFetchRequest) Decode(pd decoder.PacketDecoder) error {
	var err error
	if r.ConsumerGroup, err = pd.GetString(); err != nil {
		return err
	}
	var partitionCount int
	if partitionCount, err = pd.GetArrayLength(); err != nil {
		return err
	}
	if partitionCount <= 0 {
		return nil
	}
	r.Partitions = make(map[string][]int32, partitionCount)
	for i := 0; i < partitionCount; i++ {
		var topic string
		if topic, err = pd.GetString(); err != nil {
			return err
		}

		var partitions []int32
		if partitions, err = pd.GetCompactInt32Array(); err != nil {
			return err
		}

		r.Partitions[topic] = partitions
	}
	return nil
}

func (r *OffsetFetchRequest) APIKey() int16 {
	return 9
}

func (r *OffsetFetchRequest) APIVersion() int16 {
	return 1
}
