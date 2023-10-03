package coder

import (
	"encoding/binary"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
)

// LengthField 用于计算4字节长度。
type LengthField struct {
	startOffset int
	length      int32
}

func (l *LengthField) Decode(pd decoder.PacketDecoder) error {
	var err error
	l.length, err = pd.GetInt32()
	if err != nil {
		return err
	}
	if l.length > int32(pd.Remaining()) {
		return util.ErrInsufficientData
	}
	return nil
}

func (l *LengthField) SaveOffset(in int) {
	l.startOffset = in
}

func (l *LengthField) ReserveLength() int {
	return 4
}

func (l *LengthField) Run(curOffset int, buf []byte) error {
	binary.BigEndian.PutUint32(buf[l.startOffset:], uint32(curOffset-l.startOffset-4))
	return nil
}

func (l *LengthField) Check(curOffset int, buf []byte) error {
	if int32(curOffset-l.startOffset-4) != l.length {
		return util.PacketDecodingError{Info: "length field invalid"}
	}

	return nil
}
