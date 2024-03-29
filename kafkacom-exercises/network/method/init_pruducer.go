package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
)

// InitProducer 初始化生产者
func InitProducer(broker *meta.Broker) (*response.InitProducerIDResponse, error) {
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody:  &request.InitProducerIDRequest{},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return nil, err
	}

	responseBody := response.InitProducerIDResponse{}
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
