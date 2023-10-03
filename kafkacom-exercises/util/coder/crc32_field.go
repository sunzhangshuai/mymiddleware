package coder

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"kafkacom-exercises/util"
	"sync"
)

type crcPolynomial int8

const (
	CRCIEEE crcPolynomial = iota
	CRCCastagnoli
)

var crc32FieldPool = sync.Pool{}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func AcquireCrc32Field(polynomial crcPolynomial) *Crc32Field {
	val := crc32FieldPool.Get()
	if val != nil {
		c := val.(*Crc32Field)
		c.polynomial = polynomial
		return c
	}
	return NewCRC32Field(polynomial)
}

func ReleaseCrc32Field(c *Crc32Field) {
	crc32FieldPool.Put(c)
}

// Crc32Field implements the pushEncoder and pushDecoder interfaces for calculating CRC32s.
type Crc32Field struct {
	startOffset int
	polynomial  crcPolynomial
}

func NewCRC32Field(polynomial crcPolynomial) *Crc32Field {
	return &Crc32Field{polynomial: polynomial}
}

func (c *Crc32Field) SaveOffset(in int) {
	c.startOffset = in
}

func (c *Crc32Field) ReserveLength() int {
	return 4
}

func (c *Crc32Field) Run(curOffset int, buf []byte) error {
	crc, err := c.crc(curOffset, buf)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(buf[c.startOffset:], crc)
	return nil
}

func (c *Crc32Field) Check(curOffset int, buf []byte) error {
	crc, err := c.crc(curOffset, buf)
	if err != nil {
		return err
	}

	expected := binary.BigEndian.Uint32(buf[c.startOffset:])
	if crc != expected {
		return util.PacketDecodingError{Info: fmt.Sprintf("CRC didn't match expected %#x got %#x", expected, crc)}
	}

	return nil
}

func (c *Crc32Field) crc(curOffset int, buf []byte) (uint32, error) {
	var tab *crc32.Table
	switch c.polynomial {
	case CRCIEEE:
		tab = crc32.IEEETable
	case CRCCastagnoli:
		tab = castagnoliTable
	default:
		return 0, util.PacketDecodingError{Info: "invalid CRC type"}
	}
	return crc32.Checksum(buf[c.startOffset+4:curOffset], tab), nil
}
