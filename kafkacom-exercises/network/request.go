package network

import (
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	encoder "kafkacom-exercises/util/coder/encoder"
	"net"
)

// ProtocolBody 协议内容
type ProtocolBody interface {
	encoder.Encoder
	decoder.Decoder
	APIKey() int16 // API类型编号
	APIVersion() int16
}

type Request struct {
	CorrelationID int32  // 请求的ID，用于匹配请求和响应
	ClientID      string // 客户端ID，标识每个客户端的唯一性
	Callback      func(response *Response) error
	ProtocolBody  ProtocolBody // 请求的类型，包含
}

// Encode 请求体格式：长度，2字节请求key[相当于method]，2字节版本号，4字节会话ID，客户端id，请求内容
func (r *Request) Encode(pe encoder.PacketEncoder) error {
	// 用4个字节计算字符总长度，push占位
	pe.Push(&coder.LengthField{})

	pe.PutInt16(r.ProtocolBody.APIKey())
	pe.PutInt16(r.ProtocolBody.APIVersion())
	pe.PutInt32(r.CorrelationID)

	if err := pe.PutString(r.ClientID); err != nil {
		return err
	}

	err := r.ProtocolBody.Encode(pe)
	if err != nil {
		return err
	}

	// pop 向占位的4个字节中长度信息
	return pe.Pop()
}

// Send 发送消息
func (r *Request) Send(conn net.Conn) error {
	buf, err := coder.Encode(r)
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	return nil
}
