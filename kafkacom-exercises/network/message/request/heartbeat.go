package request

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// HeartbeatRequest 心跳请求结果
type HeartbeatRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID *string
}

func (r *HeartbeatRequest) Encode(pe encoder.PacketEncoder) error {
	if err := pe.PutString(r.GroupID); err != nil {
		return err
	}
	pe.PutInt32(r.GenerationID)
	if err := pe.PutString(r.MemberID); err != nil {
		return err
	}
	return nil
}

func (r *HeartbeatRequest) Decode(pd decoder.PacketDecoder) error {
	var err error
	if r.GroupID, err = pd.GetString(); err != nil {
		return err
	}
	if r.GenerationID, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.MemberID, err = pd.GetString(); err != nil {
		return err
	}

	return nil
}

func (r *HeartbeatRequest) APIKey() int16 {
	return 12
}

func (r *HeartbeatRequest) APIVersion() int16 {
	return 2
}
