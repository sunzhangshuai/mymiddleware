package meta

import (
	"encoding/binary"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

const (
	isTransactionalMask   = 0x10
	controlMask           = 0x20
	maximumRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1
)

// RecordHeader 存储记录头的键和值
type RecordHeader struct {
	Key   []byte // 键
	Value []byte // 值
}

func (h *RecordHeader) encode(pe encoder.PacketEncoder) error {
	if err := pe.PutVarintBytes(h.Key); err != nil {
		return err
	}
	return pe.PutVarintBytes(h.Value)
}

func (h *RecordHeader) decode(pd decoder.PacketDecoder) (err error) {
	if h.Key, err = pd.GetVarintBytes(); err != nil {
		return err
	}

	if h.Value, err = pd.GetVarintBytes(); err != nil {
		return err
	}
	return nil
}

// Record is kafka record type
type Record struct {
	Headers []*RecordHeader // 消息头

	Attributes     int8          // 暂没看到使用场景
	TimestampDelta time.Duration // 延时
	OffsetDelta    int64         // 批次中每个记录相对于FirstOffset的偏移量。
	Key            []byte
	Value          []byte
	length         coder.VarintLengthField // 动态长度
}

func (r *Record) Encode(pe encoder.PacketEncoder) error {
	// 记录长度
	pe.Push(&r.length)
	// 记录属性
	pe.PutInt8(r.Attributes)
	// 记录延时
	pe.PutVarint(int64(r.TimestampDelta / time.Millisecond))
	// 记录相对偏移量
	pe.PutVarint(r.OffsetDelta)
	// 记录key
	if err := pe.PutVarintBytes(r.Key); err != nil {
		return err
	}
	// 记录内容
	if err := pe.PutVarintBytes(r.Value); err != nil {
		return err
	}

	// 记录头长度
	pe.PutVarint(int64(len(r.Headers)))

	// 记录头
	for _, h := range r.Headers {
		if err := h.encode(pe); err != nil {
			return err
		}
	}

	// 计算长度
	return pe.Pop()
}

func (r *Record) Decode(pd decoder.PacketDecoder) (err error) {
	// 记录长度
	if err = pd.Push(&r.length); err != nil {
		return err
	}

	// 记录属性
	if r.Attributes, err = pd.GetInt8(); err != nil {
		return err
	}

	// 记录延时
	timestamp, err := pd.GetVarint()
	if err != nil {
		return err
	}
	r.TimestampDelta = time.Duration(timestamp) * time.Millisecond

	// 记录相对偏移量
	if r.OffsetDelta, err = pd.GetVarint(); err != nil {
		return err
	}

	// 记录key
	if r.Key, err = pd.GetVarintBytes(); err != nil {
		return err
	}

	// 记录内容
	if r.Value, err = pd.GetVarintBytes(); err != nil {
		return err
	}

	// 记录头
	numHeaders, err := pd.GetVarint()
	if err != nil {
		return err
	}

	if numHeaders >= 0 {
		r.Headers = make([]*RecordHeader, numHeaders)
	}
	for i := int64(0); i < numHeaders; i++ {
		hdr := new(RecordHeader)
		if err := hdr.decode(pd); err != nil {
			return err
		}
		r.Headers[i] = hdr
	}

	return pd.Pop()
}
