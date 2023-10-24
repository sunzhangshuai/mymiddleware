package method

import (
	"fmt"
	"github.com/pkg/errors"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
	"kafkacom-exercises/util"
)

// FetchMessages 获取信息
func FetchMessages(broker *meta.Broker, offsets OffsetBlocks) (OffsetBlocks, error) {
	body := &request.FetchRequest{
		MinBytes:    1,
		MaxWaitTime: 500,
		Isolation:   request.ReadUncommitted,
		// 未实现
		SessionID:    0,
		SessionEpoch: -1,
	}
	for _, offset := range offsets {
		body.AddBlock(offset.Topic, offset.Partition, offset.Offset, 1024*1024*1024, offset.LeaderEpoch)
	}
	if len(body.Blocks) == 0 {
		return nil, nil
	}
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody:  body,
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return nil, err
	}
	responseBody := response.FetchResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: &responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- n

	// 需要同步等待结果
	<-n.SyncSign
	close(n.SyncSign)

	result := make(OffsetBlocks, 0)
	for topic, ps := range responseBody.Blocks {
		for num, block := range ps {
			messages, offset, err := parseTopicPartitionRecords(topic, num, offsets.FindBlock(topic, num).Offset, block)
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("topic: %s, partition: %d", topic, num))
			}
			result = append(result, &OffsetBlock{
				Topic:       topic,
				Partition:   num,
				Offset:      offset,
				LeaderEpoch: offsets.FindBlock(topic, num).LeaderEpoch,
				Messages:    messages,
			})
		}
	}

	return result, nil
}

// parseTopicPartitionRecords 格式化分区消息
func parseTopicPartitionRecords(topic string, partition int32, offset int64,
	block *response.FetchResponseBlock) ([]*meta.Message, int64, error) {
	// 有错抛出
	if !errors.Is(block.Err, util.ErrNoError) {
		return nil, offset, block.Err
	}

	var messages []*meta.Message
	for _, records := range block.RecordsSet {
		var messageSetMessages []*meta.Message
		messageSetMessages, offset = parseBatchRecords(topic, partition, offset, records)
		messages = append(messages, messageSetMessages...)
	}

	return messages, offset, nil
}

// parseBatchRecords 格式化消息批
func parseBatchRecords(topic string, partition int32, offset int64, batch *meta.RecordBatch) ([]*meta.Message, int64) {
	messages := make([]*meta.Message, 0, len(batch.Records))
	var resOffset int64

	for _, rec := range batch.Records {
		curOffset := batch.FirstOffset + rec.OffsetDelta
		if curOffset < offset {
			continue
		}
		timestamp := batch.FirstTimestamp.Add(rec.TimestampDelta)
		if batch.LogAppendTime {
			timestamp = batch.MaxTimestamp
		}

		var key *string
		if len(rec.Key) > 0 {
			k := string(rec.Key)
			key = &k
		}
		messages = append(messages, &meta.Message{
			Topic:        topic,
			PartitionNum: partition,
			Key:          key,
			Value:        string(rec.Value),
			Offset:       curOffset,
			Timestamp:    timestamp,
			Headers:      rec.Headers,
		})

		resOffset = curOffset + 1
	}
	if len(messages) == 0 {
		resOffset++
	}
	return messages, resOffset
}
