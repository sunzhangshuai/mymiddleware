package meta

import (
	"errors"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

const recordBatchOverhead = 49

// Record数组
type recordsArray []*Record

func (e recordsArray) Encode(pe encoder.PacketEncoder) error {
	for _, r := range e {
		if err := r.Encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (e recordsArray) Decode(pd decoder.PacketDecoder) error {
	for i := range e {
		rec := &Record{}
		if err := rec.Decode(pd); err != nil {
			return err
		}
		e[i] = rec
	}
	return nil
}

type RecordBatch struct {
	FirstOffset           int64                 // 消费者用
	PartitionLeaderEpoch  int32                 // 暂未看到使用
	Version               int8                  // 版本，用2
	Codec                 util.CompressionCodec // 压缩方式，不压缩
	CompressionLevel      int                   // 压缩等级，或者后面处理
	Control               bool                  // attribute 的首位代表控制
	LogAppendTime         bool                  // 是否有LogAppendTime
	LastOffsetDelta       int32                 // 最后的相对偏移数
	FirstTimestamp        time.Time
	MaxTimestamp          time.Time
	ProducerID            int64 // 生产者ID
	ProducerEpoch         int16 // 生产者纪元
	FirstSequence         int32
	Records               []*Record
	PartialTrailingRecord bool // 是否只有部分数据
	IsTransactional       bool // 是否是事务

	compressedRecords []byte
	recordsLen        int // uncompressed records size
}

func (b *RecordBatch) LastOffset() int64 {
	return b.FirstOffset + int64(b.LastOffsetDelta)
}

func (b *RecordBatch) Encode(pe encoder.PacketEncoder) error {
	// 初始offset
	pe.PutInt64(b.FirstOffset)
	// 包长度
	pe.Push(&coder.LengthField{})
	// 暂无用
	pe.PutInt32(b.PartitionLeaderEpoch)
	// 消息版本
	pe.PutInt8(b.Version)
	// 编码
	pe.Push(coder.NewCRC32Field(coder.CRCCastagnoli))
	// 属性
	pe.PutInt16(b.computeAttributes())
	// 消息偏移
	pe.PutInt32(b.LastOffsetDelta)

	// 时间
	if err := (coder.Timestamp{Time: &b.FirstTimestamp}).Encode(pe); err != nil {
		return err
	}

	if err := (coder.Timestamp{Time: &b.MaxTimestamp}).Encode(pe); err != nil {
		return err
	}

	// producer相关
	pe.PutInt64(b.ProducerID)
	pe.PutInt16(b.ProducerEpoch)
	pe.PutInt32(b.FirstSequence)

	// 记录长度
	if err := pe.PutArrayLength(len(b.Records)); err != nil {
		return err
	}

	// 记录内容
	if b.compressedRecords == nil {
		if err := b.encodeRecords(); err != nil {
			return err
		}
	}
	if err := pe.PutRawBytes(b.compressedRecords); err != nil {
		return err
	}

	if err := pe.Pop(); err != nil {
		return err
	}
	return pe.Pop()
}

func (b *RecordBatch) Decode(pd decoder.PacketDecoder) (err error) {
	// 初始offset
	if b.FirstOffset, err = pd.GetInt64(); err != nil {
		return err
	}

	batchLen, err := pd.GetInt32()
	if err != nil {
		return err
	}

	if b.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		return err
	}

	if b.Version, err = pd.GetInt8(); err != nil {
		return err
	}

	crc32Decoder := coder.AcquireCrc32Field(coder.CRCCastagnoli)
	defer coder.ReleaseCrc32Field(crc32Decoder)

	if err = pd.Push(crc32Decoder); err != nil {
		return err
	}

	// 解析属性
	attributes, err := pd.GetInt16()
	if err != nil {
		return err
	}
	b.Codec = util.CompressionCodec(int8(attributes) & util.CompressionCodecMask)
	b.Control = attributes&controlMask == controlMask
	b.LogAppendTime = attributes&util.TimestampTypeMask == util.TimestampTypeMask
	b.IsTransactional = attributes&isTransactionalMask == isTransactionalMask

	if b.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		return err
	}

	if err = (coder.Timestamp{Time: &b.FirstTimestamp}).Decode(pd); err != nil {
		return err
	}

	if err = (coder.Timestamp{Time: &b.MaxTimestamp}).Decode(pd); err != nil {
		return err
	}

	// product相关
	if b.ProducerID, err = pd.GetInt64(); err != nil {
		return err
	}

	if b.ProducerEpoch, err = pd.GetInt16(); err != nil {
		return err
	}

	if b.FirstSequence, err = pd.GetInt32(); err != nil {
		return err
	}

	// 结果
	numRecs, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	if numRecs >= 0 {
		b.Records = make([]*Record, numRecs)
	}

	bufSize := int(batchLen) - recordBatchOverhead
	recBuffer, err := pd.GetRawBytes(bufSize)
	if err != nil {
		if errors.Is(err, util.ErrInsufficientData) {
			b.PartialTrailingRecord = true
			b.Records = nil
			return nil
		}
		return err
	}

	if err = pd.Pop(); err != nil {
		return err
	}

	// 暂不解压缩
	// recBuffer, err = decompress(b.Codec, recBuffer)
	if err != nil {
		return err
	}

	b.recordsLen = len(recBuffer)
	err = coder.Decode(recBuffer, recordsArray(b.Records))
	if errors.Is(err, util.ErrInsufficientData) {
		b.PartialTrailingRecord = true
		b.Records = nil
		return nil
	}
	return err
}

func (b *RecordBatch) encodeRecords() error {
	var raw []byte
	var err error
	if raw, err = coder.Encode(recordsArray(b.Records)); err != nil {
		return err
	}
	b.recordsLen = len(raw)

	// 暂不压缩
	// b.compressedRecords, err = compress(b.Codec, b.CompressionLevel, raw)
	b.compressedRecords = raw
	return err
}

// computeAttributes 计算属性
func (b *RecordBatch) computeAttributes() int16 {
	attr := int16(b.Codec) & int16(util.CompressionCodecMask)
	if b.Control {
		attr |= controlMask
	}
	if b.LogAppendTime {
		attr |= util.TimestampTypeMask
	}
	if b.IsTransactional {
		attr |= isTransactionalMask
	}
	return attr
}

func (b *RecordBatch) addRecord(r *Record) {
	b.Records = append(b.Records, r)
}
