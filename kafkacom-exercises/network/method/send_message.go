package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/request"
	"kafkacom-exercises/network/message/response"
)

// SendMessage 发送消息
func SendMessage(broker *meta.Broker, data map[string]map[int32]*meta.RecordBatch,
	callback func(response *network.Response) error) {
	r := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &request.ProduceRequest{
			TransactionalID: nil,
			RequiredAcks:    request.WaitForLocal,
			Timeout:         3000,
			Records:         data,
		},
		Callback: callback,
	}

	broker.Lock()
	defer broker.Unlock()
	if err := r.Send(broker.Conn); err != nil {
		return
	}

	responseBody := response.ProduceResponse{}
	n := &network.Response{
		Request:      r,
		ProtocolBody: &responseBody,
	}
	broker.Responses <- n
	return
}
