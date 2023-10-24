package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
)

// FindCoordinator 寻找协调者
func FindCoordinator(broker *meta.Broker, groupID string) (*meta.Broker, error) {
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &request.FindCoordinatorRequest{
			CoordinatorKey: groupID,
		},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return nil, err
	}

	responseBody := &response.FindCoordinatorResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- n

	// 需要同步等待结果
	<-n.SyncSign
	close(n.SyncSign)
	return responseBody.Coordinator, nil
}
