package message

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type RequiredAcks int16

const (
	// NoResponse 不需要响应
	NoResponse RequiredAcks = 0
	// WaitForLocal 仅等待本地提交成功后再进行响应。
	WaitForLocal RequiredAcks = 1
	// WaitForAll 等待所有同步副本提交后再进行响应。
	WaitForAll RequiredAcks = -1
)

type ProduceRequest struct {
	TransactionalID *string      // 暂时不用
	RequiredAcks    RequiredAcks // ack策略
	Timeout         int32        // 超时时间
	Records         map[string]map[int32]*meta.RecordBatch
}

// Encode 编码
func (p *ProduceRequest) Encode(pe encoder.PacketEncoder) (err error) {
	// 不用
	if err := pe.PutNullableString(p.TransactionalID); err != nil {
		return err
	}

	pe.PutInt16(int16(p.RequiredAcks))
	pe.PutInt32(p.Timeout)

	// 主题个数
	if err = pe.PutArrayLength(len(p.Records)); err != nil {
		return err
	}

	for topic, partitions := range p.Records {
		// 主题名称
		if err = pe.PutString(topic); err != nil {
			return err
		}

		// 分区的长度
		err = pe.PutArrayLength(len(partitions))
		if err != nil {
			return err
		}

		// 分区的长度
		for id, records := range partitions {
			// 分区ID
			pe.PutInt32(id)
			// 消息长度占位
			pe.Push(&coder.LengthField{})
			err = records.Encode(pe)
			if err != nil {
				return err
			}
			// 计算消息长度
			err = pe.Pop()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Decode 解码
func (p *ProduceRequest) Decode(pd decoder.PacketDecoder) error {
	id, err := pd.GetNullableString()
	if err != nil {
		return err
	}
	p.TransactionalID = id

	// ack 类型
	requiredAcks, err := pd.GetInt16()
	if err != nil {
		return err
	}
	p.RequiredAcks = RequiredAcks(requiredAcks)
	// 超时时间
	if p.Timeout, err = pd.GetInt32(); err != nil {
		return err
	}
	// 主题个数
	topicCount, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}

	// 初始化记录
	p.Records = make(map[string]map[int32]*meta.RecordBatch)
	for i := 0; i < topicCount; i++ {
		// 主题名称
		topic, err := pd.GetString()
		if err != nil {
			return err
		}
		// 分区长度
		partitionCount, err := pd.GetArrayLength()
		if err != nil {
			return err
		}
		p.Records[topic] = make(map[int32]*meta.RecordBatch)

		for j := 0; j < partitionCount; j++ {
			partition, err := pd.GetInt32()
			if err != nil {
				return err
			}
			size, err := pd.GetInt32()
			if err != nil {
				return err
			}
			recordsDecoder, err := pd.GetSubset(int(size))
			if err != nil {
				return err
			}
			var records *meta.RecordBatch
			if err := records.Decode(recordsDecoder); err != nil {
				return err
			}
			p.Records[topic][partition] = records
		}
	}

	return nil
}

func (p *ProduceRequest) APIKey() int16 {
	return 0
}

func (p *ProduceRequest) APIVersion() int16 {
	return 7
}
