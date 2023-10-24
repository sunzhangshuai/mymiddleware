package response

import (
	"fmt"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

// ProduceResponseBlock 分区返回值
type ProduceResponseBlock struct {
	Err         util.KError // v0, error_code
	Offset      int64       // v0, 日志偏移量
	Timestamp   time.Time   // v2, 并且代理配置的 LogAppendTime
	StartOffset int64       // v5, 日志的开始偏移量
}

func (b *ProduceResponseBlock) decode(pd decoder.PacketDecoder) (err error) {
	// 错误
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	b.Err = util.KError(tmp)

	// 偏移量
	b.Offset, err = pd.GetInt64()
	if err != nil {
		return err
	}

	// LogAppendTime
	if millis, err := pd.GetInt64(); err != nil {
		return err
	} else if millis != -1 {
		b.Timestamp = time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
	}

	// 日志的开始偏移量
	b.StartOffset, err = pd.GetInt64()
	if err != nil {
		return err
	}

	return nil
}

// Encode 编码
func (b *ProduceResponseBlock) encode(pe encoder.PacketEncoder) (err error) {
	// 错误
	pe.PutInt16(int16(b.Err))
	// 偏移
	pe.PutInt64(b.Offset)

	// 时间
	timestamp := int64(-1)
	if !b.Timestamp.Before(time.Unix(0, 0)) {
		timestamp = b.Timestamp.UnixNano() / int64(time.Millisecond)
	} else if !b.Timestamp.IsZero() {
		return util.PacketEncodingError{Info: fmt.Sprintf("invalid timestamp (%v)", b.Timestamp)}
	}
	pe.PutInt64(timestamp)

	// 最早的偏移
	pe.PutInt64(b.StartOffset)
	return nil
}

type ProduceResponse struct {
	Blocks       map[string]map[int32]*ProduceResponseBlock // v0, responses
	ThrottleTime time.Duration                              // v1, throttle_time_ms
}

// Decode 解码
func (p *ProduceResponse) Decode(pd decoder.PacketDecoder) (err error) {
	// 主题个数
	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	p.Blocks = make(map[string]map[int32]*ProduceResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		// 主题名称
		name, err := pd.GetString()
		if err != nil {
			return err
		}

		// 分区长度，因为要返回offset
		numBlocks, err := pd.GetArrayLength()
		if err != nil {
			return err
		}

		p.Blocks[name] = make(map[int32]*ProduceResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			// 分区id
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			block := new(ProduceResponseBlock)
			err = block.decode(pd)
			if err != nil {
				return err
			}
			p.Blocks[name][id] = block
		}
	}

	// 时间
	millis, err := pd.GetInt32()
	if err != nil {
		return err
	}

	p.ThrottleTime = time.Duration(millis) * time.Millisecond
	return nil
}

// Encode 编码
func (p *ProduceResponse) Encode(pe encoder.PacketEncoder) error {
	// 主题个数
	err := pe.PutArrayLength(len(p.Blocks))
	if err != nil {
		return err
	}

	for topic, partitions := range p.Blocks {
		// 主题名称
		err = pe.PutString(topic)
		if err != nil {
			return err
		}

		// 分区长度
		err = pe.PutArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for id, prb := range partitions {
			pe.PutInt32(id)
			err = prb.encode(pe)
			if err != nil {
				return err
			}
		}
	}

	pe.PutInt32(int32(p.ThrottleTime / time.Millisecond))
	return nil
}

func (p *ProduceResponse) APIKey() int16 {
	return 0
}
func (p *ProduceResponse) APIVersion() int16 {
	return 0
}
