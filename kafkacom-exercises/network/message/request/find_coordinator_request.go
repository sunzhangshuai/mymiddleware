package request

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type CoordinatorType int8

const (
	CoordinatorGroup CoordinatorType = iota
)

// FindCoordinatorRequest 协调者包含消费组和事务，这里只实现消费组
type FindCoordinatorRequest struct {
	CoordinatorKey string // GroupID 消费组名称
}

func (r *FindCoordinatorRequest) Encode(pe encoder.PacketEncoder) error {
	if err := pe.PutString(r.CoordinatorKey); err != nil {
		return err
	}
	// 协调器类型，消费组
	pe.PutInt8(int8(CoordinatorGroup))
	return nil
}

func (r *FindCoordinatorRequest) Decode(pd decoder.PacketDecoder) (err error) {
	if r.CoordinatorKey, err = pd.GetString(); err != nil {
		return err
	}
	return nil
}

func (r *FindCoordinatorRequest) APIKey() int16 {
	return 10
}

func (r *FindCoordinatorRequest) APIVersion() int16 {
	return 2
}
