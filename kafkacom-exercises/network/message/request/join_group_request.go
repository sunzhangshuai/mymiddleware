package request

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util/coder"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// GroupProtocol 组协议
type GroupProtocol struct {
	Name     string                    // 协议名称
	Metadata *meta.ConsumerGroupMember // 协议元数据
}

func (p *GroupProtocol) Encode(pe encoder.PacketEncoder) error {
	if err := pe.PutString(p.Name); err != nil {
		return err
	}
	bin, err := coder.Encode(p.Metadata)
	if err != nil {
		return err
	}
	return pe.PutBytes(bin)
}

func (p *GroupProtocol) Decode(pd decoder.PacketDecoder) error {
	var err error
	if p.Name, err = pd.GetString(); err != nil {
		return err
	}
	bin, err := pd.GetBytes()
	if err != nil {
		return err
	}
	p.Metadata = new(meta.ConsumerGroupMember)
	return coder.Decode(bin, p.Metadata)
}

// JoinGroupRequest 加入组请求
type JoinGroupRequest struct {
	GroupId               string           // 组标识
	SessionTimeout        int32            // 心跳超时时间
	RebalanceTimeout      int32            // 平衡组时，等待每个成员加入的最长时间
	MemberId              string           // 成员ID
	GroupInstanceId       *string          // 包含最终用户提供的使用者实例的唯一标识符。
	ProtocolType          string           // 要加入的组实现的协议类的唯一名称
	OrderedGroupProtocols []*GroupProtocol // 包含成员支持使用的协议的有序列表。
}

func (r *JoinGroupRequest) Encode(pe encoder.PacketEncoder) error {
	// 组ID
	if err := pe.PutString(r.GroupId); err != nil {
		return err
	}
	// 超时时间
	pe.PutInt32(r.SessionTimeout)
	pe.PutInt32(r.RebalanceTimeout)
	// 组成员ID
	if err := pe.PutString(r.MemberId); err != nil {
		return err
	}
	if err := pe.PutNullableString(r.GroupInstanceId); err != nil {
		return err
	}

	// 组协议
	if err := pe.PutString(r.ProtocolType); err != nil {
		return err
	}

	// 成员支持使用的协议的有序列表
	if err := pe.PutArrayLength(len(r.OrderedGroupProtocols)); err != nil {
		return err
	}
	for _, protocol := range r.OrderedGroupProtocols {
		if err := protocol.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (r *JoinGroupRequest) Decode(pd decoder.PacketDecoder) (err error) {
	if r.GroupId, err = pd.GetString(); err != nil {
		return
	}
	if r.SessionTimeout, err = pd.GetInt32(); err != nil {
		return
	}
	if r.RebalanceTimeout, err = pd.GetInt32(); err != nil {
		return err
	}

	if r.MemberId, err = pd.GetString(); err != nil {
		return
	}
	if r.GroupInstanceId, err = pd.GetNullableString(); err != nil {
		return
	}

	if r.ProtocolType, err = pd.GetString(); err != nil {
		return
	}
	n, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		protocol := &GroupProtocol{}
		if err := protocol.Decode(pd); err != nil {
			return err
		}
		r.OrderedGroupProtocols = append(r.OrderedGroupProtocols, protocol)
	}
	return nil
}

func (r *JoinGroupRequest) APIKey() int16 {
	return 11
}

func (r *JoinGroupRequest) APIVersion() int16 {
	return 5
}
