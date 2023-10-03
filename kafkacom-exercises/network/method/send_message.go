package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message"
)

// SendMessage 发送消息
func SendMessage(broker *meta.Broker, data map[string]map[int32]*meta.RecordBatch,
	callback func(response *network.Response) error) {
	request := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody: &message.ProduceRequest{
			TransactionalID: nil,
			RequiredAcks:    message.WaitForAll,
			Timeout:         0,
			Records:         data,
		},
		Callback: callback,
	}

	broker.Lock()
	defer broker.Unlock()
	if err := request.Send(broker.Conn); err != nil {
		return
	}

	responseBody := message.ProduceResponse{}
	response := &network.Response{
		Request:      request,
		ProtocolBody: &responseBody,
	}
	broker.Responses <- response
	return
}
