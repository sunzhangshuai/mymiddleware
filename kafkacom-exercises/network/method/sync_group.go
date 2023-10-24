package method

import (
	"kafkacom-exercises/consumer/balance"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
)

// SyncGroup 同步组信息
func SyncGroup(broker *meta.Broker, groupID string, memberID string, generationId int32,
	plan balance.StrategyPlan) (*meta.ConsumerGroupMemberAssignment, error) {

	// 整理分配内容
	var groupAssignments []*request.SyncGroupRequestAssignment
	if plan != nil {
		groupAssignments = make([]*request.SyncGroupRequestAssignment, 0, len(plan))
		for mID, topics := range plan {
			groupAssignments = append(groupAssignments, &request.SyncGroupRequestAssignment{
				MemberId: mID,
				Assignment: &meta.ConsumerGroupMemberAssignment{
					Topics: topics,
				},
			})
		}
	}

	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &request.SyncGroupRequest{
			GroupId:          groupID,
			MemberId:         memberID,
			GenerationId:     generationId,
			GroupAssignments: groupAssignments,
		},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return nil, err
	}

	responseBody := &response.SyncGroupResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- n

	// 需要同步等待结果
	<-n.SyncSign
	close(n.SyncSign)
	return responseBody.MemberAssignment, nil
}
