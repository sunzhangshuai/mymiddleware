package message

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type MetadataRequest struct {
	// Topics 包含要为其获取元数据的主题。
	Topics []string
	// AllowAutoTopicCreation true：代理可能会自动创建我们请求的不存在的主题，如果它被配置为这样做的话。
	AllowAutoTopicCreation bool
}

func (r *MetadataRequest) Encode(pe encoder.PacketEncoder) (err error) {
	// 可以选择只获取某个主题，全部传个 -1
	if len(r.Topics) > 0 {
		err := pe.PutArrayLength(len(r.Topics))
		if err != nil {
			return err
		}

		for i := range r.Topics {
			err = pe.PutString(r.Topics[i])
			if err != nil {
				return err
			}
		}
	} else {
		pe.PutInt32(-1)
	}

	// 是否自动创建
	pe.PutBool(r.AllowAutoTopicCreation)

	return nil
}

func (r *MetadataRequest) Decode(pd decoder.PacketDecoder) (err error) {
	size, err := pd.GetInt32()
	if err != nil {
		return err
	}
	if size > 0 {
		r.Topics = make([]string, size)
		for i := range r.Topics {
			topic, err := pd.GetString()
			if err != nil {
				return err
			}
			r.Topics[i] = topic
		}
	}

	if r.AllowAutoTopicCreation, err = pd.GetBool(); err != nil {
		return err
	}

	return nil
}

func (r *MetadataRequest) APIKey() int16 {
	return 3
}

func (r *MetadataRequest) APIVersion() int16 {
	return 7
}
