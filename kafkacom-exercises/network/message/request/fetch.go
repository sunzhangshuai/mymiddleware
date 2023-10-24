package request

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// FetchRequestBlock 分区拉取块儿
type FetchRequestBlock struct {
	currentLeaderEpoch int32 // 包含分区的当前前导epoch。
	fetchOffset        int64 // 拉取偏移量
	logStartOffset     int64 // 包含跟随副本的最早可用偏移。该字段仅在跟随者发送请求时使用。
	maxBytes           int32 // 包含要从此分区获取的最大字节数。有关可能不遵守此限制的情况，请参阅KIP-74。
}

func (b *FetchRequestBlock) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt32(b.currentLeaderEpoch)
	pe.PutInt64(b.fetchOffset)
	pe.PutInt64(b.logStartOffset)
	pe.PutInt32(b.maxBytes)
	return nil
}

func (b *FetchRequestBlock) Decode(pd decoder.PacketDecoder) error {
	var err error
	if b.currentLeaderEpoch, err = pd.GetInt32(); err != nil {
		return err
	}
	if b.fetchOffset, err = pd.GetInt64(); err != nil {
		return err
	}
	if b.logStartOffset, err = pd.GetInt64(); err != nil {
		return err
	}
	if b.maxBytes, err = pd.GetInt32(); err != nil {
		return err
	}
	return nil
}

// FetchRequest (API key 1) will fetch Kafka messages. Version 3 introduced the MaxBytes field. See
// https://issues.apache.org/jira/browse/KAFKA-2063 for a discussion of the issues leading up to that.  The KIP is at
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes
type FetchRequest struct {
	MaxWaitTime int32 // 包含等待响应的最长时间（以毫秒为单位）。
	MinBytes    int32 // 包含要在响应中累积的最小字节。
	MaxBytes    int32 // 包含要获取的最大字节数。有关可能不遵守此限制的情况，请参阅KIP-74。
	// 隔离包含一个此设置控制事务记录的可见性。
	// 使用READ_UNCOMITTED（isolation_level=0）可使所有记录可见。
	// 使用READ_COMMITTED（isolation_level=1），可以看到非事务性和COMMITTED事务性记录。
	// 更具体地说，READ_COMMITED从小于当前LSO（最后一个稳定的偏移量）的偏移量返回所有数据，并允许在结果中包含中止的事务列表，这允许消费者丢弃中止的事务记录隔离包含此设置控制事务记录的可见性。
	// 使用READ_UNCOMITTED（isolation_level=0）可使所有记录可见。使用READ_COMMITTED（isolation_level=1），可以看到非事务性和COMMITTED事务性记录。
	// 更具体地说，READ_COMMITED从小于当前LSO（最后一个稳定的偏移量）的偏移量返回所有数据，并允许在结果中包含中止事务的列表，允许消费者丢弃已中止的交易记录。
	Isolation    IsolationLevel // 隔离级别
	SessionID    int32          // 会话ID
	SessionEpoch int32          // 包含跟随者副本或使用者已知的分区前导的epoch。
	// Blocks contains the topics to fetch.
	Blocks    map[string]map[int32]*FetchRequestBlock
	forgotten map[string][]int32 // 包含忽略的分区
	RackID    string             // 包含提出此请求的消费者的机架ID。
}

// IsolationLevel 隔离级别
type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = iota // 可使所有记录可见。
	ReadCommitted                         // 可以看到非事务性和COMMITTED事务性记录。
)

func (r *FetchRequest) Encode(pe encoder.PacketEncoder) error {
	var err error
	// 客户端的ReplicaID始终为-1
	pe.PutInt32(-1)
	pe.PutInt32(r.MaxWaitTime)
	pe.PutInt32(r.MinBytes)
	pe.PutInt32(r.MaxBytes)
	pe.PutInt8(int8(r.Isolation))
	pe.PutInt32(r.SessionID)
	pe.PutInt32(r.SessionEpoch)

	// topic block
	if err = pe.PutArrayLength(len(r.Blocks)); err != nil {
		return err
	}
	for topic, blocks := range r.Blocks {
		if err = pe.PutString(topic); err != nil {
			return err
		}
		// partition block
		if err = pe.PutArrayLength(len(blocks)); err != nil {
			return err
		}
		for partition, block := range blocks {
			pe.PutInt32(partition)
			if err = block.Encode(pe); err != nil {
				return err
			}
		}
	}
	// 忽略的分区列表
	if err = pe.PutArrayLength(len(r.forgotten)); err != nil {
		return err
	}
	for topic, partitions := range r.forgotten {
		if err = pe.PutString(topic); err != nil {
			return err
		}
		if err = pe.PutArrayLength(len(partitions)); err != nil {
			return err
		}
		for _, partition := range partitions {
			pe.PutInt32(partition)
		}
	}

	// 机架ID
	if err = pe.PutString(r.RackID); err != nil {
		return err
	}
	return nil
}

func (r *FetchRequest) Decode(pd decoder.PacketDecoder) error {
	var err error

	if _, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.MaxWaitTime, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.MinBytes, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.MaxBytes, err = pd.GetInt32(); err != nil {
		return err
	}
	isolation, err := pd.GetInt8()
	if err != nil {
		return err
	}
	r.Isolation = IsolationLevel(isolation)
	if r.SessionID, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.SessionEpoch, err = pd.GetInt32(); err != nil {
		return err
	}
	topicCount, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	// block
	r.Blocks = make(map[string]map[int32]*FetchRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.GetString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.GetArrayLength()
		if err != nil {
			return err
		}
		r.Blocks[topic] = make(map[int32]*FetchRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.GetInt32()
			if err != nil {
				return err
			}
			fetchBlock := &FetchRequestBlock{}
			if err = fetchBlock.Decode(pd); err != nil {
				return err
			}
			r.Blocks[topic][partition] = fetchBlock
		}
	}

	// forgotten
	forgottenCount, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	r.forgotten = make(map[string][]int32)
	for i := 0; i < forgottenCount; i++ {
		topic, err := pd.GetString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.GetArrayLength()
		if err != nil {
			return err
		}
		r.forgotten[topic] = make([]int32, partitionCount)

		for j := 0; j < partitionCount; j++ {
			partition, err := pd.GetInt32()
			if err != nil {
				return err
			}
			r.forgotten[topic][j] = partition
		}
	}

	// 机架
	r.RackID, err = pd.GetString()
	if err != nil {
		return err
	}
	return nil
}

func (r *FetchRequest) APIKey() int16 {
	return 1
}

func (r *FetchRequest) APIVersion() int16 {
	return 10
}

func (r *FetchRequest) AddBlock(topic string, partitionID int32, fetchOffset int64, maxBytes int32, leaderEpoch int32) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*FetchRequestBlock)
	}
	if r.forgotten == nil {
		r.forgotten = make(map[string][]int32)
	}
	if r.Blocks[topic] == nil {
		r.Blocks[topic] = make(map[int32]*FetchRequestBlock)
	}
	r.Blocks[topic][partitionID] = &FetchRequestBlock{
		currentLeaderEpoch: leaderEpoch,
		fetchOffset:        fetchOffset,
		maxBytes:           maxBytes,
	}
}
