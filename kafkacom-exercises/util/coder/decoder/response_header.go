package decoder

import (
	"fmt"
	"kafkacom-exercises/util"
)

type ResponseHeader struct {
	Length        int32
	CorrelationID int32
}

// Decode 响应头解析
func (r *ResponseHeader) Decode(pd PacketDecoder) (err error) {
	// 获取长度
	r.Length, err = pd.GetInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 || r.Length > util.MaxResponseSize {
		return util.PacketDecodingError{Info: fmt.Sprintf("message of Length %d too large or too small", r.Length)}
	}

	// 获取会话id
	r.CorrelationID, err = pd.GetInt32()
	return err
}
