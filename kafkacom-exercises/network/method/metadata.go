package method

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	request2 "kafkacom-exercises/network/message/request"
	message "kafkacom-exercises/network/message/response"
)

func GetMetaData(broker *meta.Broker, f func(response *network.Response) error) error {
	// 发送请求
	request := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		Callback:      f,
		ProtocolBody:  &request2.MetadataRequest{},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := request.Send(broker.Conn); err != nil {
		return err
	}

	// 等待结果
	response := &network.Response{
		Request:      request,
		ProtocolBody: &message.MetadataResponse{},
	}
	broker.Responses <- response
	return nil
}

// GetMetaDataSync 同步获取元数据
func GetMetaDataSync(broker *meta.Broker, f func(response *network.Response) error) error {
	// 发送请求
	request := &network.Request{
		CorrelationID: broker.CorrelationID,
		ClientID:      broker.ClientID,
		ProtocolBody:  &request2.MetadataRequest{},
	}

	broker.Lock()
	defer broker.Unlock()
	if err := request.Send(broker.Conn); err != nil {
		return err
	}

	// 等待结果
	response := &network.Response{
		Request:      request,
		ProtocolBody: &message.MetadataResponse{},
		SyncSign:     make(chan struct{}),
	}
	broker.Responses <- response

	<-response.SyncSign
	close(response.SyncSign)
	return f(response)
}
