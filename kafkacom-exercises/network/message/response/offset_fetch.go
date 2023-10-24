package response

import (
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// OffsetFetchResponseBlock 每一个元素
type OffsetFetchResponseBlock struct {
	Offset      int64  // 偏移量
	LeaderEpoch int32  // 主节点
	Metadata    string // 元数据
	Err         util.KError
}

func (b *OffsetFetchResponseBlock) Encode(pe encoder.PacketEncoder) (err error) {
	pe.PutInt64(b.Offset)
	if err = pe.PutString(b.Metadata); err != nil {
		return err
	}
	pe.PutInt16(int16(b.Err))
	return nil
}

func (b *OffsetFetchResponseBlock) Decode(pd decoder.PacketDecoder) error {
	var err error

	if b.Offset, err = pd.GetInt64(); err != nil {
		return err
	}
	if b.Metadata, err = pd.GetString(); err != nil {
		return err
	}
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	b.Err = util.KError(tmp)
	return nil
}

// OffsetFetchResponse 由协调者偏移量结果
type OffsetFetchResponse struct {
	Blocks map[string]map[int32]*OffsetFetchResponseBlock // 结果
	Err    util.KError                                    // 错误
}

func (r *OffsetFetchResponse) Encode(pe encoder.PacketEncoder) error {
	var err error
	if err = pe.PutArrayLength(len(r.Blocks)); err != nil {
		return err
	}

	for topic, partitions := range r.Blocks {
		if err = pe.PutString(topic); err != nil {
			return err
		}
		if err = pe.PutArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.PutInt32(partition)
			if err = block.Encode(pe); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetFetchResponse) Decode(pd decoder.PacketDecoder) error {
	var err error
	var numTopics int
	if numTopics, err = pd.GetArrayLength(); err != nil {
		return err
	}

	if numTopics > 0 {
		r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock, numTopics)
		for i := 0; i < numTopics; i++ {
			var name string
			if name, err = pd.GetString(); err != nil {
				return err
			}

			var numBlocks int
			if numBlocks, err = pd.GetArrayLength(); err != nil {
				return err
			}

			r.Blocks[name] = nil
			if numBlocks > 0 {
				r.Blocks[name] = make(map[int32]*OffsetFetchResponseBlock, numBlocks)
			}
			for j := 0; j < numBlocks; j++ {
				var id int32
				if id, err = pd.GetInt32(); err != nil {
					return err
				}

				block := new(OffsetFetchResponseBlock)
				if err = block.Decode(pd); err != nil {
					return err
				}
				r.Blocks[name][id] = block
			}
		}
	}
	return nil
}

func (r *OffsetFetchResponse) APIKey() int16 {
	return 9
}

func (r *OffsetFetchResponse) APIVersion() int16 {
	return 1
}
