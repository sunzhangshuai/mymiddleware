package network

import (
	"io"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder"
	decoder "kafkacom-exercises/util/coder/decoder"
	"net"
)

type Response struct {
	Request      *Request
	ProtocolBody ProtocolBody

	// 解决同步问题
	SyncSign chan struct{}
}

// Decode 解析返回数据
func (r *Response) Decode(conn net.Conn) error {
	// 获取头
	header := make([]byte, util.HeaderLength)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return err
	}
	decodedHeader := decoder.ResponseHeader{}
	err = coder.Decode(header, &decodedHeader)

	// 获取内容
	buf := make([]byte, decodedHeader.Length-int32(util.HeaderLength)+4)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return err
	}

	err = coder.Decode(buf, r.ProtocolBody)
	return nil
}
