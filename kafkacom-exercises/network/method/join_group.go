package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
	"time"
)

// JoinGroup 加入组
func JoinGroup(broker *meta.Broker, groupID string, memberID string, topics []string, sessionTimeout time.Duration,
	rebalanceTimeout time.Duration, balanceName string) (*response.JoinGroupResponse, error) {
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &request.JoinGroupRequest{
			GroupId:          groupID,
			MemberId:         memberID,
			SessionTimeout:   int32(sessionTimeout / time.Millisecond),
			ProtocolType:     "consumer",
			RebalanceTimeout: int32(rebalanceTimeout / time.Millisecond),
			OrderedGroupProtocols: []*request.GroupProtocol{{
				Name: balanceName,
				Metadata: &meta.ConsumerGroupMember{
					Topics: topics,
				},
			}},
		},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return nil, err
	}

	responseBody := response.JoinGroupResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: &responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- n

	// 需要同步等待结果
	<-n.SyncSign
	close(n.SyncSign)
	return &responseBody, nil
}
