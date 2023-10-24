package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
)

// Heartbeat 心跳
func Heartbeat(broker *meta.Broker, groupID string, memberID string, generationID int32) error {
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &request.HeartbeatRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
		},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return err
	}

	responseBody := &response.HeartbeatResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- n

	// 需要同步等待结果
	<-n.SyncSign
	close(n.SyncSign)
	return responseBody.Err
}
