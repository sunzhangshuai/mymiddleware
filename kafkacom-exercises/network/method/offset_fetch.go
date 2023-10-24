package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
)

// OffsetFetch 获取偏移量
func OffsetFetch(broker *meta.Broker, groupID string,
	partitions map[string][]int32) (OffsetBlocks, error) {
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &request.OffsetFetchRequest{
			ConsumerGroup: groupID,
			Partitions:    partitions,
		},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return nil, err
	}

	responseBody := response.OffsetFetchResponse{}
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
	for t, m := range responseBody.Blocks {
		for p, b := range m {
			result = append(result, &OffsetBlock{
				Topic:     t,
				Partition: p,
				Offset:    b.Offset,
				Metadata:  b.Metadata,
			})
		}
	}
	return result, nil
}
