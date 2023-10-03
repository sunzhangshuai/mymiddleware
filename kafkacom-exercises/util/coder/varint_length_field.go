package coder

import (
	"encoding/binary"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
)

type VarintLengthField struct {
	startOffset int
	length      int64
}

func (l *VarintLengthField) Decode(pd decoder.PacketDecoder) error {
	var err error
	l.length, err = pd.GetVarint()
	return err
}

func (l *VarintLengthField) SaveOffset(in int) {
	l.startOffset = in
}

func (l *VarintLengthField) AdjustLength(currOffset int) int {
	oldFieldSize := l.ReserveLength()
	l.length = int64(currOffset - l.startOffset - oldFieldSize)

	return l.ReserveLength() - oldFieldSize
}

func (l *VarintLengthField) ReserveLength() int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutVarint(tmp[:], l.length)
}

func (l *VarintLengthField) Run(curOffset int, buf []byte) error {
	binary.PutVarint(buf[l.startOffset:], l.length)
	return nil
}

func (l *VarintLengthField) Check(curOffset int, buf []byte) error {
	if int64(curOffset-l.startOffset-l.ReserveLength()) != l.length {
		return util.PacketDecodingError{Info: "length field invalid"}
	}

	return nil
}
