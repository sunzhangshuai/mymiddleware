package encoder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"kafkacom-exercises/util"
	"math"
)

type PrepEncoder struct {
	Stack  []PushEncoder
	Length int
}

// primitives

func (pe *PrepEncoder) PutInt8(int8) {
	pe.Length++
}

func (pe *PrepEncoder) PutInt16(int16) {
	pe.Length += 2
}

func (pe *PrepEncoder) PutInt32(int32) {
	pe.Length += 4
}

func (pe *PrepEncoder) PutInt64(int64) {
	pe.Length += 8
}

func (pe *PrepEncoder) PutVarint(in int64) {
	var buf [binary.MaxVarintLen64]byte
	pe.Length += binary.PutVarint(buf[:], in)
}

func (pe *PrepEncoder) PutUVarint(in uint64) {
	var buf [binary.MaxVarintLen64]byte
	pe.Length += binary.PutUvarint(buf[:], in)
}

func (pe *PrepEncoder) PutFloat64(float64) {
	pe.Length += 8
}

func (pe *PrepEncoder) PutArrayLength(in int) error {
	if in > math.MaxInt32 {
		return util.PacketEncodingError{Info: fmt.Sprintf("array too long (%d)", in)}
	}
	pe.Length += 4
	return nil
}

func (pe *PrepEncoder) PutCompactArrayLength(in int) {
	pe.PutUVarint(uint64(in + 1))
}

func (pe *PrepEncoder) PutBool(bool) {
	pe.Length++
}

// arrays

func (pe *PrepEncoder) PutBytes(in []byte) error {
	pe.Length += 4
	if in == nil {
		return nil
	}
	return pe.PutRawBytes(in)
}

func (pe *PrepEncoder) PutVarintBytes(in []byte) error {
	if in == nil {
		pe.PutVarint(-1)
		return nil
	}
	pe.PutVarint(int64(len(in)))
	return pe.PutRawBytes(in)
}

func (pe *PrepEncoder) PutCompactBytes(in []byte) error {
	pe.PutUVarint(uint64(len(in) + 1))
	return pe.PutRawBytes(in)
}

func (pe *PrepEncoder) PutCompactString(in string) error {
	pe.PutCompactArrayLength(len(in))
	return pe.PutRawBytes([]byte(in))
}

func (pe *PrepEncoder) PutNullableCompactString(in *string) error {
	if in == nil {
		pe.PutUVarint(0)
		return nil
	} else {
		return pe.PutCompactString(*in)
	}
}

func (pe *PrepEncoder) PutRawBytes(in []byte) error {
	if len(in) > math.MaxInt32 {
		return util.PacketEncodingError{Info: fmt.Sprintf("byteslice too long (%d)", len(in))}
	}
	pe.Length += len(in)
	return nil
}

func (pe *PrepEncoder) PutNullableString(in *string) error {
	if in == nil {
		pe.Length += 2
		return nil
	}
	return pe.PutString(*in)
}

func (pe *PrepEncoder) PutString(in string) error {
	pe.Length += 2
	if len(in) > math.MaxInt16 {
		return util.PacketEncodingError{Info: fmt.Sprintf("string too long (%d)", len(in))}
	}
	pe.Length += len(in)
	return nil
}

func (pe *PrepEncoder) PutStringArray(in []string) error {
	err := pe.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, str := range in {
		if err := pe.PutString(str); err != nil {
			return err
		}
	}

	return nil
}

func (pe *PrepEncoder) PutCompactInt32Array(in []int32) error {
	if in == nil {
		return errors.New("expected int32 array to be non null")
	}

	pe.PutUVarint(uint64(len(in)) + 1)
	pe.Length += 4 * len(in)
	return nil
}

func (pe *PrepEncoder) PutNullableCompactInt32Array(in []int32) error {
	if in == nil {
		pe.PutUVarint(0)
		return nil
	}

	pe.PutUVarint(uint64(len(in)) + 1)
	pe.Length += 4 * len(in)
	return nil
}

func (pe *PrepEncoder) PutInt32Array(in []int32) error {
	err := pe.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	pe.Length += 4 * len(in)
	return nil
}

func (pe *PrepEncoder) PutInt64Array(in []int64) error {
	err := pe.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	pe.Length += 8 * len(in)
	return nil
}

func (pe *PrepEncoder) PutEmptyTaggedFieldArray() {
	pe.PutUVarint(0)
}

func (pe *PrepEncoder) Offset() int {
	return pe.Length
}

// stackable

func (pe *PrepEncoder) Push(in PushEncoder) {
	in.SaveOffset(pe.Length)
	pe.Length += in.ReserveLength()
	pe.Stack = append(pe.Stack, in)
}

func (pe *PrepEncoder) Pop() error {
	in := pe.Stack[len(pe.Stack)-1]
	pe.Stack = pe.Stack[:len(pe.Stack)-1]
	if dpe, ok := in.(DynamicPushEncoder); ok {
		pe.Length += dpe.AdjustLength(pe.Length)
	}

	return nil
}
