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

type OffsetBlocks []*OffsetBlock

type OffsetBlock struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    string
	Messages    []*meta.Message
}

func (o *OffsetBlocks) FindBlock(topic string, partition int32) *OffsetBlock {
	for _, block := range *o {
		if block.Topic == topic && block.Partition == partition {
			return block
		}
	}
	return nil
}

// OffsetCommit 提交偏移量
func OffsetCommit(broker *meta.Broker, groupID string, memberID string, generationID int32, blocks []*OffsetBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	body := &request.OffsetCommitRequest{
		ConsumerGroup:           groupID,
		ConsumerGroupGeneration: generationID,
		ConsumerID:              memberID,
	}

	for _, block := range blocks {
		body.AddBlockWithLeaderEpoch(block.Topic, block.Partition, block.Offset, block.Metadata)
	}

	//a, _ := json.Marshal(body)
	//fmt.Printf("[%s]提交offset的参数:%s\n", time.Now().Format("04:05.000"), string(a))
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody:  body,
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return err
	}

	responseBody := &response.OffsetCommitResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- n

	// 需要同步等待结果
	<-n.SyncSign
	close(n.SyncSign)
	for t, m := range responseBody.Errors {
		for p, kError := range m {
			if kError != util.ErrNoError {
				return errors.Wrap(kError, fmt.Sprintf("topic: %s, partition: %d", t, p))
			}
		}
	}
	return nil
}
