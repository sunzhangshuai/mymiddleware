package decoder

type PushDecoder interface {
	// SaveOffset 将偏移量保存到输入缓冲区中，作为实际写入计算值的位置（如果可能）。
	SaveOffset(in int)

	// ReserveLength 返回为该编码器的输出保留的数据长度 (eg 4 bytes for a CRC32).
	ReserveLength() int

	// Check 指示所有必需的数据现在都可用于计算和检查字段。
	// SaveOffset保证首先被调用。实现应该从保存的偏移量中读取 ReserveLength() 字节的数据，
	// 并根据保存的偏移和curOffset之间的数据进行验证。
	Check(curOffset int, buf []byte) error
}

type Decoder interface {
	Decode(pd PacketDecoder) error
}

// DynamicPushDecoder 扩展了pushDecoder的接口，用于字段本身的长度在其值被解码之前是未知的情况（例如可变编码长度字段）。
// 在 push 时，将调用 DynamicPushDecoder.Decode() 方法，而不是 ReserveLength（）
type DynamicPushDecoder interface {
	PushDecoder
	Decoder
}

// PacketDecoder 是为阅读Kafka的编码规则提供帮助的接口。
// 实现Decoder的类型只需要担心调用GetString之类的方法，而不需要担心字符串在Kafka中是如何表示的。
type PacketDecoder interface {
	// GetInt8 基础数据格式
	GetInt8() (int8, error)
	GetInt16() (int16, error)
	GetInt32() (int32, error)
	GetInt64() (int64, error)
	GetVarint() (int64, error)
	GetUVarint() (uint64, error)
	GetFloat64() (float64, error)
	GetArrayLength() (int, error)
	GetCompactArrayLength() (int, error)
	GetBool() (bool, error)
	GetEmptyTaggedFieldArray() (int, error)

	// GetBytes 聚合数据格式
	GetBytes() ([]byte, error)
	GetVarintBytes() ([]byte, error)
	GetCompactBytes() ([]byte, error)
	GetRawBytes(length int) ([]byte, error)
	GetString() (string, error)
	GetNullableString() (*string, error)
	GetCompactString() (string, error)
	GetCompactNullableString() (*string, error)
	GetCompactInt32Array() ([]int32, error)
	GetInt32Array() ([]int32, error)
	GetInt64Array() ([]int64, error)
	GetStringArray() ([]string, error)

	// Remaining 用于计算剩余内容，只查看不获取，类似于queue的top
	Remaining() int                                 // 剩余字节数
	GetSubset(length int) (PacketDecoder, error)    // 获取剩余
	Peek(offset, length int) (PacketDecoder, error) // 类似于 GetSubset，但它不会提前偏移
	PeekInt8(offset int) (int8, error)              // 类似于 peek，但只有一个字节

	// Push Pop 如果前面占位的字符需要后面的元素计算的话，可以用push和pop进行处理
	Push(in PushDecoder) error
	Pop() error
}
