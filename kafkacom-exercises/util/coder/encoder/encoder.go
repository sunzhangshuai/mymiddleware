package encoder

// PushEncoder 是对CRC和长度等字段进行编码的接口，其中字段的值取决于数据包中其后的编码内容。
// 用实际值位于数据包中的 PacketEncoder.Push() 启动它们，其中 实际值位于数据包中，然后在写入它们所依赖的所有字节时用 PacketEncoder.Pop() 启动。
type PushEncoder interface {
	// SaveOffset 将偏移量保存到输入缓冲区中，作为实际写入计算值的位置（如果可能）。
	SaveOffset(in int)

	// ReserveLength 返回为该编码器的输出保留的数据长度 (eg 4 bytes for a CRC32).
	ReserveLength() int

	// Run 指示所有必需的数据现在都可用于计算和写入字段。
	// SaveOffset 保证首先被调用。实现应该写入 ReserveLength() 字节
	// 基于保存的偏移量和curOffset之间的数据，将数据的偏移量转换为保存的偏移。
	Run(curOffset int, buf []byte) error
}

type Encoder interface {
	Encode(pe PacketEncoder) error
}

// DynamicPushEncoder 扩展了pushEncoder的接口，用于字段本身的长度在计算其值之前未知的情况（例如variant编码的长度字段）。
type DynamicPushEncoder interface {
	PushEncoder

	// AdjustLength 在 pop() 期间调用以调整字段的长度。
	// 它应该返回上次计算的长度和当前长度之间的差值（以字节为单位）。
	AdjustLength(currOffset int) int
}

// PacketEncoder 是为使用Kafka的编码规则进行编写提供帮助的接口。
// 实现Encoder的类型只需要担心调用PutString等方法，而不是关于字符串在Kafka中是如何表示的。
type PacketEncoder interface {
	// 基本数据类型
	PutInt8(in int8)
	PutInt16(in int16)
	PutInt32(in int32)
	PutInt64(in int64)
	PutVarint(in int64)
	PutUVarint(in uint64)
	PutFloat64(in float64)
	PutCompactArrayLength(in int)
	PutArrayLength(in int) error
	PutBool(in bool)

	// 复杂数据类型
	PutBytes(in []byte) error
	PutVarintBytes(in []byte) error
	PutCompactBytes(in []byte) error
	PutRawBytes(in []byte) error
	PutCompactString(in string) error
	PutNullableCompactString(in *string) error
	PutString(in string) error
	PutNullableString(in *string) error
	PutStringArray(in []string) error
	PutCompactInt32Array(in []int32) error
	PutNullableCompactInt32Array(in []int32) error
	PutInt32Array(in []int32) error
	PutInt64Array(in []int64) error
	PutEmptyTaggedFieldArray()

	// Offset Provide the current offset to record the batch size metric
	Offset() int

	// Stacks, see PushEncoder
	Push(in PushEncoder)
	Pop() error
}
