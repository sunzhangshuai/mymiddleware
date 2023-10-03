package decoder

import (
	"encoding/binary"
	"kafkacom-exercises/util"
	"math"
)

var (
	errInvalidArrayLength     = util.PacketDecodingError{Info: "invalid array Length"}
	errInvalidByteSliceLength = util.PacketDecodingError{Info: "invalid byteslice Length"}
	errInvalidStringLength    = util.PacketDecodingError{Info: "invalid string Length"}
	errVarintOverflow         = util.PacketDecodingError{Info: "varint overflow"}
	errUVarintOverflow        = util.PacketDecodingError{Info: "uvarint overflow"}
	errInvalidBool            = util.PacketDecodingError{Info: "invalid bool"}
)

type RealDecoder struct {
	Raw   []byte
	Off   int // 已解析的字节数
	stack []PushDecoder
}

// primitives

func (rd *RealDecoder) GetInt8() (int8, error) {
	if rd.Remaining() < 1 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	tmp := int8(rd.Raw[rd.Off])
	rd.Off++
	return tmp, nil
}

func (rd *RealDecoder) GetInt16() (int16, error) {
	if rd.Remaining() < 2 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.Raw[rd.Off:]))
	rd.Off += 2
	return tmp, nil
}

func (rd *RealDecoder) GetInt32() (int32, error) {
	if rd.Remaining() < 4 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.Raw[rd.Off:]))
	rd.Off += 4
	return tmp, nil
}

func (rd *RealDecoder) GetInt64() (int64, error) {
	if rd.Remaining() < 8 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.Raw[rd.Off:]))
	rd.Off += 8
	return tmp, nil
}

func (rd *RealDecoder) GetVarint() (int64, error) {
	tmp, n := binary.Varint(rd.Raw[rd.Off:])
	if n == 0 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	if n < 0 {
		rd.Off -= n
		return -1, errVarintOverflow
	}
	rd.Off += n
	return tmp, nil
}

func (rd *RealDecoder) GetUVarint() (uint64, error) {
	tmp, n := binary.Uvarint(rd.Raw[rd.Off:])
	if n == 0 {
		rd.Off = len(rd.Raw)
		return 0, util.ErrInsufficientData
	}

	if n < 0 {
		rd.Off -= n
		return 0, errUVarintOverflow
	}

	rd.Off += n
	return tmp, nil
}

func (rd *RealDecoder) GetFloat64() (float64, error) {
	if rd.Remaining() < 8 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	tmp := math.Float64frombits(binary.BigEndian.Uint64(rd.Raw[rd.Off:]))
	rd.Off += 8
	return tmp, nil
}

func (rd *RealDecoder) GetArrayLength() (int, error) {
	if rd.Remaining() < 4 {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	}
	tmp := int(int32(binary.BigEndian.Uint32(rd.Raw[rd.Off:])))
	rd.Off += 4
	if tmp > rd.Remaining() {
		rd.Off = len(rd.Raw)
		return -1, util.ErrInsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, errInvalidArrayLength
	}
	return tmp, nil
}

func (rd *RealDecoder) GetCompactArrayLength() (int, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (rd *RealDecoder) GetBool() (bool, error) {
	b, err := rd.GetInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errInvalidBool
	}
	return true, nil
}

func (rd *RealDecoder) GetEmptyTaggedFieldArray() (int, error) {
	tagCount, err := rd.GetUVarint()
	if err != nil {
		return 0, err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := rd.GetUVarint()
		if err != nil {
			return 0, err
		}
		length, err := rd.GetUVarint()
		if err != nil {
			return 0, err
		}
		if _, err := rd.GetRawBytes(int(length)); err != nil {
			return 0, err
		}
	}

	return 0, nil
}

// collections

func (rd *RealDecoder) GetBytes() ([]byte, error) {
	tmp, err := rd.GetInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(tmp))
}

func (rd *RealDecoder) GetVarintBytes() ([]byte, error) {
	tmp, err := rd.GetVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(tmp))
}

func (rd *RealDecoder) GetCompactBytes() ([]byte, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)
	return rd.GetRawBytes(length)
}

func (rd *RealDecoder) GetStringLength() (int, error) {
	length, err := rd.GetInt16()
	if err != nil {
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errInvalidStringLength
	case n > rd.Remaining():
		rd.Off = len(rd.Raw)
		return 0, util.ErrInsufficientData
	}

	return n, nil
}

func (rd *RealDecoder) GetString() (string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	tmpStr := string(rd.Raw[rd.Off : rd.Off+n])
	rd.Off += n
	return tmpStr, nil
}

func (rd *RealDecoder) GetNullableString() (*string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	tmpStr := string(rd.Raw[rd.Off : rd.Off+n])
	rd.Off += n
	return &tmpStr, err
}

func (rd *RealDecoder) GetCompactString() (string, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return "", err
	}

	length := int(n - 1)
	if length < 0 {
		return "", errInvalidByteSliceLength
	}
	tmpStr := string(rd.Raw[rd.Off : rd.Off+length])
	rd.Off += length
	return tmpStr, nil
}

func (rd *RealDecoder) GetCompactNullableString() (*string, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)

	if length < 0 {
		return nil, err
	}

	tmpStr := string(rd.Raw[rd.Off : rd.Off+length])
	rd.Off += length
	return &tmpStr, err
}

func (rd *RealDecoder) GetCompactInt32Array() ([]int32, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	arrayLength := int(n) - 1

	ret := make([]int32, arrayLength)

	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.Raw[rd.Off:]))
		rd.Off += 4
	}
	return ret, nil
}

func (rd *RealDecoder) GetInt32Array() ([]int32, error) {
	if rd.Remaining() < 4 {
		rd.Off = len(rd.Raw)
		return nil, util.ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.Raw[rd.Off:]))
	rd.Off += 4

	if rd.Remaining() < 4*n {
		rd.Off = len(rd.Raw)
		return nil, util.ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int32, n)
	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.Raw[rd.Off:]))
		rd.Off += 4
	}
	return ret, nil
}

func (rd *RealDecoder) GetInt64Array() ([]int64, error) {
	if rd.Remaining() < 4 {
		rd.Off = len(rd.Raw)
		return nil, util.ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.Raw[rd.Off:]))
	rd.Off += 4

	if rd.Remaining() < 8*n {
		rd.Off = len(rd.Raw)
		return nil, util.ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int64, n)
	for i := range ret {
		ret[i] = int64(binary.BigEndian.Uint64(rd.Raw[rd.Off:]))
		rd.Off += 8
	}
	return ret, nil
}

func (rd *RealDecoder) GetStringArray() ([]string, error) {
	if rd.Remaining() < 4 {
		rd.Off = len(rd.Raw)
		return nil, util.ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.Raw[rd.Off:]))
	rd.Off += 4

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]string, n)
	for i := range ret {
		str, err := rd.GetString()
		if err != nil {
			return nil, err
		}

		ret[i] = str
	}
	return ret, nil
}

// subsets

func (rd *RealDecoder) Remaining() int {
	return len(rd.Raw) - rd.Off
}

func (rd *RealDecoder) GetSubset(length int) (PacketDecoder, error) {
	buf, err := rd.GetRawBytes(length)
	if err != nil {
		return nil, err
	}
	return &RealDecoder{Raw: buf}, nil
}

func (rd *RealDecoder) GetRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errInvalidByteSliceLength
	} else if length > rd.Remaining() {
		rd.Off = len(rd.Raw)
		return nil, util.ErrInsufficientData
	}

	start := rd.Off
	rd.Off += length
	return rd.Raw[start:rd.Off], nil
}

func (rd *RealDecoder) Peek(offset, length int) (PacketDecoder, error) {
	if rd.Remaining() < offset+length {
		return nil, util.ErrInsufficientData
	}
	off := rd.Off + offset
	return &RealDecoder{Raw: rd.Raw[off : off+length]}, nil
}

func (rd *RealDecoder) PeekInt8(offset int) (int8, error) {
	const byteLen = 1
	if rd.Remaining() < offset+byteLen {
		return -1, util.ErrInsufficientData
	}
	return int8(rd.Raw[rd.Off+offset]), nil
}

// stacks

func (rd *RealDecoder) Push(in PushDecoder) error {
	in.SaveOffset(rd.Off)

	var reserve int
	if dpd, ok := in.(DynamicPushDecoder); ok {
		if err := dpd.Decode(rd); err != nil {
			return err
		}
	} else {
		reserve = in.ReserveLength()
		if rd.Remaining() < reserve {
			rd.Off = len(rd.Raw)
			return util.ErrInsufficientData
		}
	}

	rd.stack = append(rd.stack, in)

	rd.Off += reserve

	return nil
}

func (rd *RealDecoder) Pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := rd.stack[len(rd.stack)-1]
	rd.stack = rd.stack[:len(rd.stack)-1]

	return in.Check(rd.Off, rd.Raw)
}
