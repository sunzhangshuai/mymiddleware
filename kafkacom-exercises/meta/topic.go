package meta

import (
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// TopicMetadata 主题的内容
type TopicMetadata struct {
	// Err 错误码
	Err util.KError
	// Name 主题名称
	Name string
	// IsInternal 是否为内部主题
	IsInternal bool
	// Partitions 分区信息
	Partitions []*PartitionMetadata
}

func (t *TopicMetadata) Decode(pd decoder.PacketDecoder) (err error) {
	// 前两个字节是错误码
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	t.Err = util.KError(tmp)

	// 主题名称
	if t.Name, err = pd.GetString(); err != nil {
		return err
	}

	// 是否为内部主题
	if t.IsInternal, err = pd.GetBool(); err != nil {
		return err
	}

	// 分区信息
	if numPartitions, err := pd.GetArrayLength(); err != nil {
		return err
	} else {
		t.Partitions = make([]*PartitionMetadata, numPartitions)
		for i := 0; i < numPartitions; i++ {
			block := &PartitionMetadata{}
			if err := block.Decode(pd); err != nil {
				return err
			}
			t.Partitions[i] = block
		}
	}

	return nil
}

func (t *TopicMetadata) Encode(pe encoder.PacketEncoder) (err error) {
	pe.PutInt16(int16(t.Err))

	if err := pe.PutString(t.Name); err != nil {
		return err
	}

	pe.PutBool(t.IsInternal)

	if err := pe.PutArrayLength(len(t.Partitions)); err != nil {
		return err
	}
	for _, block := range t.Partitions {
		if err := block.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}
