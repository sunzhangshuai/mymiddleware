package util

import "fmt"

const (
	// CompressionNone no compression
	CompressionNone CompressionCodec = iota
	// CompressionGZIP compression using GZIP
	CompressionGZIP
	// CompressionSnappy compression using snappy
	CompressionSnappy
	// CompressionLZ4 compression using LZ4
	CompressionLZ4
	// CompressionZSTD compression using ZSTD
	CompressionZSTD

	// The lowest 3 bits contain the compression codec used for the message
	CompressionCodecMask int8 = 0x07

	// Bit 3 set for "LogAppend" timestamps
	TimestampTypeMask = 0x08

	// CompressionLevelDefault is the constant to use in CompressionLevel
	// to have the default compression level for any codec. The value is picked
	// that we don't use any existing compression levels.
	CompressionLevelDefault = -1000
)

// CompressionCodec represents the various compression codecs recognized by Kafka in messages.
type CompressionCodec int8

func (cc CompressionCodec) String() string {
	return []string{
		"none",
		"gzip",
		"snappy",
		"lz4",
		"zstd",
	}[int(cc)]
}

// UnmarshalText returns a CompressionCodec from its string representation.
func (cc *CompressionCodec) UnmarshalText(text []byte) error {
	codecs := map[string]CompressionCodec{
		"none":   CompressionNone,
		"gzip":   CompressionGZIP,
		"snappy": CompressionSnappy,
		"lz4":    CompressionLZ4,
		"zstd":   CompressionZSTD,
	}
	codec, ok := codecs[string(text)]
	if !ok {
		return fmt.Errorf("cannot parse %q as a compression codec", string(text))
	}
	*cc = codec
	return nil
}

// MarshalText transforms a CompressionCodec into its string representation.
func (cc CompressionCodec) MarshalText() ([]byte, error) {
	return []byte(cc.String()), nil
}
