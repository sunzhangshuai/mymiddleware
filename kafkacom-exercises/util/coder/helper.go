package coder

import (
	"fmt"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// Encode 采用编码器并将其转换为字节，同时可能记录度量。
func Encode(e encoder.Encoder) ([]byte, error) {
	if e == nil {
		return nil, nil
	}

	var prepEnc encoder.PrepEncoder
	var realEnc encoder.RealEncoder

	err := e.Encode(&prepEnc)
	if err != nil {
		return nil, err
	}

	if prepEnc.Length < 0 || prepEnc.Length > int(util.MaxRequestSize) {
		return nil, util.PacketEncodingError{Info: fmt.Sprintf("invalid message size (%d)", prepEnc.Length)}
	}

	// 真实编码
	realEnc.Raw = make([]byte, prepEnc.Length)
	err = e.Encode(&realEnc)
	if err != nil {
		return nil, err
	}

	return realEnc.Raw, nil
}

// Decode 解码
func Decode(buf []byte, in decoder.Decoder) error {
	if buf == nil {
		return nil
	}

	helper := decoder.RealDecoder{
		Raw: buf,
	}
	err := in.Decode(&helper)
	if err != nil {
		return err
	}

	// 处理的字符长度和期望长度不匹配
	if helper.Off != len(buf) {
		return util.PacketDecodingError{
			Info: fmt.Sprintf("invalid length (off=%d, len=%d)", helper.Off, len(buf)),
		}
	}
	return nil
}
