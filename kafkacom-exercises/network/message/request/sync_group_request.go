package request

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// SyncGroupRequestAssignment 同步组请求的分配结果
type SyncGroupRequestAssignment struct {
	MemberId   string                              // 组成员ID
	Assignment *meta.ConsumerGroupMemberAssignment // 赋值
}

func (a *SyncGroupRequestAssignment) Encode(pe encoder.PacketEncoder) error {
	var err error
	if err = pe.PutString(a.MemberId); err != nil {
		return err
	}
	if bin, err := coder.Encode(a.Assignment); err != nil {
		return err
	} else {
		return pe.PutBytes(bin)
	}
}

func (a *SyncGroupRequestAssignment) Decode(pd decoder.PacketDecoder) error {
	var err error

	if a.MemberId, err = pd.GetString(); err != nil {
		return err
	}
	if bin, err := pd.GetBytes(); err != nil {
		return err
	} else {
		a.Assignment = new(meta.ConsumerGroupMemberAssignment)
		return coder.Decode(bin, a.Assignment)
	}
}

// SyncGroupRequest 同步组请求
type SyncGroupRequest struct {
	GroupId          string                        // 组ID
	GenerationId     int32                         // 唯一ID
	MemberId         string                        // 成员ID
	GroupInstanceId  *string                       // 最终用户提供的使用者实例的唯一标识符。
	GroupAssignments []*SyncGroupRequestAssignment // 包含每个赋值。
}

func (r *SyncGroupRequest) Encode(pe encoder.PacketEncoder) error {
	var err error
	// 组与成员ID
	if err = pe.PutString(r.GroupId); err != nil {
		return err
	}
	pe.PutInt32(r.GenerationId)
	if err = pe.PutString(r.MemberId); err != nil {
		return err
	}
	if err = pe.PutNullableString(r.GroupInstanceId); err != nil {
		return err
	}

	// 分配结果
	if err = pe.PutArrayLength(len(r.GroupAssignments)); err != nil {
		return err
	}
	for _, block := range r.GroupAssignments {
		if err = block.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (r *SyncGroupRequest) Decode(pd decoder.PacketDecoder) error {
	var err error
	// ID相关
	if r.GroupId, err = pd.GetString(); err != nil {
		return err
	}
	if r.GenerationId, err = pd.GetInt32(); err != nil {
		return err
	}
	if r.MemberId, err = pd.GetString(); err != nil {
		return err
	}
	if r.GroupInstanceId, err = pd.GetNullableString(); err != nil {
		return err
	}

	// 分配结果
	var numAssignments int
	if numAssignments, err = pd.GetArrayLength(); err != nil {
		return err
	} else if numAssignments > 0 {
		r.GroupAssignments = make([]*SyncGroupRequestAssignment, numAssignments)
		for i := 0; i < numAssignments; i++ {
			block := new(SyncGroupRequestAssignment)
			if err := block.Decode(pd); err != nil {
				return err
			}
			r.GroupAssignments[i] = block
		}
	}

	return nil
}

func (r *SyncGroupRequest) APIKey() int16 {
	return 14
}

func (r *SyncGroupRequest) APIVersion() int16 {
	return 3
}
