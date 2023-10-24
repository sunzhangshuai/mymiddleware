package encoder

import (
	"encoding/binary"
	"errors"
	"math"
)

type RealEncoder struct {
	Raw   []byte // 编码分配的内存块
	off   int    // 已经编码的字节数
	stack []PushEncoder
}

// primitives

func (re *RealEncoder) PutInt8(in int8) {
	re.Raw[re.off] = byte(in)
	re.off++
}

func (re *RealEncoder) PutInt16(in int16) {
	binary.BigEndian.PutUint16(re.Raw[re.off:], uint16(in))
	re.off += 2
}

func (re *RealEncoder) PutInt32(in int32) {
	binary.BigEndian.PutUint32(re.Raw[re.off:], uint32(in))
	re.off += 4
}

func (re *RealEncoder) PutInt64(in int64) {
	binary.BigEndian.PutUint64(re.Raw[re.off:], uint64(in))
	re.off += 8
}

func (re *RealEncoder) PutVarint(in int64) {
	re.off += binary.PutVarint(re.Raw[re.off:], in)
}

func (re *RealEncoder) PutUVarint(in uint64) {
	re.off += binary.PutUvarint(re.Raw[re.off:], in)
}

func (re *RealEncoder) PutFloat64(in float64) {
	binary.BigEndian.PutUint64(re.Raw[re.off:], math.Float64bits(in))
	re.off += 8
}

func (re *RealEncoder) PutArrayLength(in int) error {
	re.PutInt32(int32(in))
	return nil
}

func (re *RealEncoder) PutCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(in + 1))
}

func (re *RealEncoder) PutBool(in bool) {
	if in {
		re.PutInt8(1)
		return
	}
	re.PutInt8(0)
}

// collection

func (re *RealEncoder) PutRawBytes(in []byte) error {
	copy(re.Raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *RealEncoder) PutBytes(in []byte) error {
	if in == nil {
		re.PutInt32(-1)
		return nil
	}
	re.PutInt32(int32(len(in)))
	return re.PutRawBytes(in)
}

func (re *RealEncoder) PutVarintBytes(in []byte) error {
	if in == nil {
		re.PutVarint(-1)
		return nil
	}
	re.PutVarint(int64(len(in)))
	return re.PutRawBytes(in)
}

func (re *RealEncoder) PutCompactBytes(in []byte) error {
	re.PutUVarint(uint64(len(in) + 1))
	return re.PutRawBytes(in)
}

func (re *RealEncoder) PutCompactString(in string) error {
	re.PutCompactArrayLength(len(in))
	return re.PutRawBytes([]byte(in))
}

func (re *RealEncoder) PutNullableCompactString(in *string) error {
	if in == nil {
		re.PutInt8(0)
		return nil
	}
	return re.PutCompactString(*in)
}

func (re *RealEncoder) PutString(in string) error {
	re.PutInt16(int16(len(in)))
	copy(re.Raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *RealEncoder) PutNullableString(in *string) error {
	if in == nil {
		re.PutInt16(-1)
		return nil
	}
	return re.PutString(*in)
}

func (re *RealEncoder) PutStringArray(in []string) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := re.PutString(val); err != nil {
			return err
		}
	}

	return nil
}

func (re *RealEncoder) PutCompactInt32Array(in []int32) error {
	if in == nil {
		return errors.New("expected int32 array to be non null")
	}
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *RealEncoder) PutNullableCompactInt32Array(in []int32) error {
	if in == nil {
		re.PutUVarint(0)
		return nil
	}
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *RealEncoder) PutInt32Array(in []int32) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *RealEncoder) PutInt64Array(in []int64) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.PutInt64(val)
	}
	return nil
}

func (re *RealEncoder) PutEmptyTaggedFieldArray() {
	re.PutUVarint(0)
}

func (re *RealEncoder) Offset() int {
	return re.off
}

// stacks

func (re *RealEncoder) Push(in PushEncoder) {
	in.SaveOffset(re.off)
	re.off += in.ReserveLength()
	re.stack = append(re.stack, in)
}

func (re *RealEncoder) Pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := re.stack[len(re.stack)-1]
	re.stack = re.stack[:len(re.stack)-1]
	return in.Run(re.off, re.Raw)
}
