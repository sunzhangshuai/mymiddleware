package response

import (
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

type HeartbeatResponse struct {
	ThrottleTime time.Duration
	Err          util.KError
}

func (r *HeartbeatResponse) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	pe.PutInt16(int16(r.Err))
	return nil
}

func (r *HeartbeatResponse) Decode(pd decoder.PacketDecoder) error {
	var err error
	var t int32
	if t, err = pd.GetInt32(); err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(t) * time.Millisecond
	kerr, err := pd.GetInt16()
	if err != nil {
		return err
	}
	r.Err = util.KError(kerr)
	return nil
}

func (r *HeartbeatResponse) APIKey() int16 {
	return 12
}

func (r *HeartbeatResponse) APIVersion() int16 {
	return 3
}
