package response

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

// JoinGroupResponse 加入组返回值
type JoinGroupResponse struct {
	ThrottleTime  time.Duration // 由于配额冲突而限制请求的持续时间。
	Err           util.KError   // 错误码。
	GenerationId  int32         // 生成的会话ID。
	GroupProtocol string        // 协调器选择的组协议。
	LeaderId      string        // 组领导者。
	MemberID      string        // 协调者分配的成员ID。
	Members       []GroupMember // 每个组的成员信息。
}

// GroupMember 组成员信息
type GroupMember struct {
	MemberId        string                    // 成员ID
	GroupInstanceId *string                   // 使用者实例的唯一标识符
	Metadata        *meta.ConsumerGroupMember // 组成员元数据。
}

func (r *JoinGroupResponse) Encode(pe encoder.PacketEncoder) error {
	var err error

	pe.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	pe.PutInt16(int16(r.Err))
	pe.PutInt32(r.GenerationId)

	if err = pe.PutString(r.GroupProtocol); err != nil {
		return err
	}
	if err = pe.PutString(r.LeaderId); err != nil {
		return err
	}
	if err = pe.PutString(r.MemberID); err != nil {
		return err
	}

	if err = pe.PutArrayLength(len(r.Members)); err != nil {
		return err
	}

	for _, member := range r.Members {
		if err = pe.PutString(member.MemberId); err != nil {
			return err
		}
		if err = pe.PutNullableString(member.GroupInstanceId); err != nil {
			return err
		}
		var bin []byte
		if bin, err = coder.Encode(member.Metadata); err != nil {
			return err
		}
		if err = pe.PutBytes(bin); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupResponse) Decode(pd decoder.PacketDecoder) error {
	throttleTime, err := pd.GetInt32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	kerr, err := pd.GetInt16()
	if err != nil {
		return err
	}
	r.Err = util.KError(kerr)
	if r.GenerationId, err = pd.GetInt32(); err != nil {
		return err
	}

	if r.GroupProtocol, err = pd.GetString(); err != nil {
		return err
	}

	if r.LeaderId, err = pd.GetString(); err != nil {
		return err
	}

	if r.MemberID, err = pd.GetString(); err != nil {
		return err
	}

	n, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	r.Members = make([]GroupMember, n)
	for i := 0; i < n; i++ {
		groupMember := GroupMember{Metadata: &meta.ConsumerGroupMember{}}
		if groupMember.MemberId, err = pd.GetString(); err != nil {
			return err
		}
		if groupMember.GroupInstanceId, err = pd.GetNullableString(); err != nil {
			return err
		}
		var bin []byte
		if bin, err = pd.GetBytes(); err != nil {
			return err
		}
		groupMember.Metadata = new(meta.ConsumerGroupMember)
		if err = coder.Decode(bin, groupMember.Metadata); err != nil {
			return err
		}
		r.Members[i] = groupMember
	}

	return nil
}

func (r *JoinGroupResponse) APIKey() int16 {
	return 11
}

func (r *JoinGroupResponse) APIVersion() int16 {
	return 5
}
