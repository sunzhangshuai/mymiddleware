package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message"
)

// InitProducer 初始化生产者
func InitProducer(broker *meta.Broker) (*message.InitProducerIDResponse, error) {
	request := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody:  &message.InitProducerIDRequest{},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := request.Send(broker.Conn); err != nil {
		return nil, err
	}

	responseBody := message.InitProducerIDResponse{}
	response := &network.Response{
		Request:      request,
		ProtocolBody: &responseBody,
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- response

	// 需要同步等待结果
	<-response.SyncSign
	close(response.SyncSign)
	return &responseBody, nil
}
