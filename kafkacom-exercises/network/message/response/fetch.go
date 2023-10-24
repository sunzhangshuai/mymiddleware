package response

import (
	"errors"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

const (
	invalidLeaderEpoch        = -1
	invalidPreferredReplicaID = -1
)

// AbortedTransaction 中止的事务
type AbortedTransaction struct {
	ProducerID  int64 // 包含与中止的事务相关联的生产者id。
	FirstOffset int64 // 包含中止事务中的第一个偏移量。
}

func (t *AbortedTransaction) Decode(pd decoder.PacketDecoder) (err error) {
	if t.ProducerID, err = pd.GetInt64(); err != nil {
		return err
	}
	if t.FirstOffset, err = pd.GetInt64(); err != nil {
		return err
	}
	return nil
}

func (t *AbortedTransaction) Encode(pe encoder.PacketEncoder) (err error) {
	pe.PutInt64(t.ProducerID)
	pe.PutInt64(t.FirstOffset)
	return nil
}

// FetchResponseBlock 结果块
type FetchResponseBlock struct {
	Err                    util.KError           // 错误码
	HighWaterMarkOffset    int64                 // 包含当前的高水位线。
	LastStableOffset       int64                 // 包含分区的最后一个稳定偏移量（或LSO）。这是最后一个偏移量，从而决定了在此偏移量之前所有事务记录的状态（ABORTED或COMMITTED）
	LastRecordsBatchOffset *int64                // 低水位
	LogStartOffset         int64                 // 包含当前日志起始偏移量。
	AbortedTransactions    []*AbortedTransaction // 包含中止的事务。
	RecordsSet             []*meta.RecordBatch   // 包含记录数据。

	Partial bool // 是否为部分结果
}

func (b *FetchResponseBlock) Decode(pd decoder.PacketDecoder) error {
	// 错误码
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	b.Err = util.KError(tmp)

	if b.HighWaterMarkOffset, err = pd.GetInt64(); err != nil {
		return err
	}
	if b.LastStableOffset, err = pd.GetInt64(); err != nil {
		return err
	}
	if b.LogStartOffset, err = pd.GetInt64(); err != nil {
		return err
	}
	numTransact, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	if numTransact >= 0 {
		b.AbortedTransactions = make([]*AbortedTransaction, numTransact)
	}

	for i := 0; i < numTransact; i++ {
		transact := new(AbortedTransaction)
		if err = transact.Decode(pd); err != nil {
			return err
		}
		b.AbortedTransactions[i] = transact
	}

	// 记录长度
	recordsSize, err := pd.GetInt32()
	if err != nil {
		return err
	}
	recordsDecoder, err := pd.GetSubset(int(recordsSize))
	if err != nil {
		return err
	}
	// 记录内容
	b.RecordsSet = []*meta.RecordBatch{}
	for recordsDecoder.Remaining() > 0 {
		records := &meta.RecordBatch{}
		if err = records.Decode(recordsDecoder); err != nil {
			// If we have at least one decoded records, this is not an error
			if errors.Is(err, util.ErrInsufficientData) {
				if len(b.RecordsSet) == 0 {
					b.Partial = true
				}
				break
			}
			return err
		}

		b.LastRecordsBatchOffset = &records.FirstOffset

		// 记录
		partial := records.PartialTrailingRecord
		n := len(records.Records)
		if n > 0 || (partial && len(b.RecordsSet) == 0) {
			b.RecordsSet = append(b.RecordsSet, records)
		}
		if partial {
			break
		}
	}

	return nil
}

func (b *FetchResponseBlock) Encode(pe encoder.PacketEncoder) (err error) {
	pe.PutInt16(int16(b.Err))
	pe.PutInt64(b.HighWaterMarkOffset)
	pe.PutInt64(b.LastStableOffset)
	pe.PutInt64(b.LogStartOffset)

	// 中断事务
	if err = pe.PutArrayLength(len(b.AbortedTransactions)); err != nil {
		return err
	}
	for _, transact := range b.AbortedTransactions {
		if err = transact.Encode(pe); err != nil {
			return err
		}
	}

	pe.Push(&coder.LengthField{})
	for _, records := range b.RecordsSet {
		err = records.Encode(pe)
		if err != nil {
			return err
		}
	}
	return pe.Pop()
}

// FetchResponse 拉取结果
type FetchResponse struct {
	ThrottleTime time.Duration                            // 限流
	ErrorCode    int16                                    // 错误码
	SessionID    int32                                    // 包含提取会话ID，如果不是提取会话的一部分，则为0。
	Blocks       map[string]map[int32]*FetchResponseBlock // 包含响应主题。

	LogAppendTime bool
	Timestamp     time.Time // 时间戳
}

func (r *FetchResponse) Decode(pd decoder.PacketDecoder) error {
	var err error
	throttle, err := pd.GetInt32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		return err
	}
	if r.SessionID, err = pd.GetInt32(); err != nil {
		return err
	}

	// topic
	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	r.Blocks = make(map[string]map[int32]*FetchResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}
		numBlocks, err := pd.GetArrayLength()
		if err != nil {
			return err
		}
		r.Blocks[name] = make(map[int32]*FetchResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}
			block := new(FetchResponseBlock)
			if err = block.Decode(pd); err != nil {
				return err
			}
			r.Blocks[name][id] = block
		}
	}

	return nil
}

func (r *FetchResponse) Encode(pe encoder.PacketEncoder) error {
	var err error
	pe.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	pe.PutInt16(r.ErrorCode)
	pe.PutInt32(r.SessionID)

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
		for id, block := range partitions {
			pe.PutInt32(id)
			if err = block.Encode(pe); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *FetchResponse) APIKey() int16 {
	return 1
}

func (r *FetchResponse) APIVersion() int16 {
	return 10
}
