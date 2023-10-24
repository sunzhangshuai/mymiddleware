package response

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type SyncGroupResponse struct {
	ThrottleTime     int32                               // 限制请求时间
	Err              util.KError                         // 错误码
	MemberAssignment *meta.ConsumerGroupMemberAssignment // 成员的分配结果
}

func (r *SyncGroupResponse) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt32(r.ThrottleTime)
	pe.PutInt16(int16(r.Err))
	if bin, err := coder.Encode(r.MemberAssignment); err != nil {
		return err
	} else {
		return pe.PutBytes(bin)
	}
}

func (r *SyncGroupResponse) Decode(pd decoder.PacketDecoder) (err error) {
	if r.ThrottleTime, err = pd.GetInt32(); err != nil {
		return err
	}
	kerr, err := pd.GetInt16()
	if err != nil {
		return err
	}

	r.Err = util.KError(kerr)

	r.MemberAssignment = new(meta.ConsumerGroupMemberAssignment)
	if bin, err := pd.GetBytes(); err != nil {
		return err
	} else {
		return coder.Decode(bin, r.MemberAssignment)
	}
}

func (r *SyncGroupResponse) APIKey() int16 {
	return 14
}

func (r *SyncGroupResponse) APIVersion() int16 {
	return 3
}
