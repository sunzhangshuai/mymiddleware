package meta

import (
	"errors"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// ConsumerGroupMember 保存使用者组的元数据
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
type ConsumerGroupMember struct {
	Version         int16             // 版本
	Topics          []string          // topic列表
	UserData        []byte            // 用户数据
	OwnedPartitions []*OwnedPartition // 分区列表
	GenerationID    int32             // 生成的member ID。
	RackID          *string           // 机架ID
}

func (m *ConsumerGroupMember) Encode(pe encoder.PacketEncoder) error {
	// 版本
	pe.PutInt16(m.Version)

	// topic名称
	if err := pe.PutStringArray(m.Topics); err != nil {
		return err
	}

	// 用户数据
	if err := pe.PutBytes(m.UserData); err != nil {
		return err
	}

	// 分配的分区
	if m.Version >= 1 {
		if err := pe.PutArrayLength(len(m.OwnedPartitions)); err != nil {
			return err
		}
		for _, op := range m.OwnedPartitions {
			if err := op.Encode(pe); err != nil {
				return err
			}
		}
	}

	// member ID
	if m.Version >= 2 {
		pe.PutInt32(m.GenerationID)
	}

	// 机架ID
	if m.Version >= 3 {
		if err := pe.PutNullableString(m.RackID); err != nil {
			return err
		}
	}
	return nil
}

func (m *ConsumerGroupMember) Decode(pd decoder.PacketDecoder) (err error) {
	// 版本
	if m.Version, err = pd.GetInt16(); err != nil {
		return
	}

	// topic名称
	if m.Topics, err = pd.GetStringArray(); err != nil {
		return
	}

	// 用户数据
	if m.UserData, err = pd.GetBytes(); err != nil {
		return
	}

	// 分配的分区
	if m.Version >= 1 {
		n, err := pd.GetArrayLength()
		if err != nil {
			// 如果行为不端的第三方客户端在其JoinGroup请求中将成员元数据错误地标记为V1，则允许此处缺少数据
			if errors.Is(err, util.ErrInsufficientData) {
				return nil
			}
			return err
		}
		if n > 0 {
			m.OwnedPartitions = make([]*OwnedPartition, n)
			for i := 0; i < n; i++ {
				m.OwnedPartitions[i] = &OwnedPartition{}
				if err := m.OwnedPartitions[i].Decode(pd); err != nil {
					return err
				}
			}
		}
	}

	// member ID
	if m.Version >= 2 {
		if m.GenerationID, err = pd.GetInt32(); err != nil {
			return err
		}
	}

	// 机架ID
	if m.Version >= 3 {
		if m.RackID, err = pd.GetNullableString(); err != nil {
			return err
		}
	}
	return nil
}

// OwnedPartition 分配的分区
type OwnedPartition struct {
	Topic      string  // 主题
	Partitions []int32 // 分区列表
}

func (m *OwnedPartition) Encode(pe encoder.PacketEncoder) error {
	if err := pe.PutString(m.Topic); err != nil {
		return err
	}
	if err := pe.PutInt32Array(m.Partitions); err != nil {
		return err
	}
	return nil
}

func (m *OwnedPartition) Decode(pd decoder.PacketDecoder) (err error) {
	if m.Topic, err = pd.GetString(); err != nil {
		return err
	}
	if m.Partitions, err = pd.GetInt32Array(); err != nil {
		return err
	}
	return nil
}

// ConsumerGroupMemberAssignment 保存消费组的成员分配
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
type ConsumerGroupMemberAssignment struct {
	Version  int16              // 版本
	Topics   map[string][]int32 // topic和分区
	UserData []byte             // 用户数据
}

func (m *ConsumerGroupMemberAssignment) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt16(m.Version)

	if err := pe.PutArrayLength(len(m.Topics)); err != nil {
		return err
	}

	for topic, partitions := range m.Topics {
		if err := pe.PutString(topic); err != nil {
			return err
		}
		if err := pe.PutInt32Array(partitions); err != nil {
			return err
		}
	}

	if err := pe.PutBytes(m.UserData); err != nil {
		return err
	}

	return nil
}

func (m *ConsumerGroupMemberAssignment) Decode(pd decoder.PacketDecoder) (err error) {
	if m.Version, err = pd.GetInt16(); err != nil {
		return
	}

	var topicLen int
	if topicLen, err = pd.GetArrayLength(); err != nil {
		return
	}

	m.Topics = make(map[string][]int32, topicLen)
	for i := 0; i < topicLen; i++ {
		var topic string
		if topic, err = pd.GetString(); err != nil {
			return
		}
		if m.Topics[topic], err = pd.GetInt32Array(); err != nil {
			return
		}
	}

	if m.UserData, err = pd.GetBytes(); err != nil {
		return
	}

	return nil
}
