package response

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

func (r *FindCoordinatorResponse) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	pe.PutInt16(int16(r.Err))
	if err := pe.PutNullableString(r.ErrMsg); err != nil {
		return err
	}
	coordinator := r.Coordinator
	if coordinator == nil {
		// 空协调者节点
		coordinator = &meta.Broker{ID: -1, Addr: ":-1"}
	}
	if err := coordinator.Encode(pe); err != nil {
		return err
	}
	return nil
}

type FindCoordinatorResponse struct {
	ThrottleTime time.Duration
	Err          util.KError
	ErrMsg       *string
	Coordinator  *meta.Broker
}

func (r *FindCoordinatorResponse) Decode(pd decoder.PacketDecoder) (err error) {
	// 毫秒级
	throttleTime, err := pd.GetInt32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	// 错误码与错误信息
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	r.Err = util.KError(tmp)
	if r.ErrMsg, err = pd.GetNullableString(); err != nil {
		return err
	}

	// 解析broker信息
	coordinator := new(meta.Broker)
	if err := coordinator.Decode(pd); err != nil {
		return err
	}
	if coordinator.Addr == ":0" {
		return nil
	}
	r.Coordinator = coordinator

	return nil
}

func (r *FindCoordinatorResponse) APIKey() int16 {
	return 10
}

func (r *FindCoordinatorResponse) APIVersion() int16 {
	return 2
}
