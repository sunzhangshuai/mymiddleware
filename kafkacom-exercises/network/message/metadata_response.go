package message

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type MetadataResponse struct {
	// ThrottleTimeMs contains the duration in milliseconds for which the req was throttled due to a quota violation, or zero if the req did not violate any quota.
	ThrottleTimeMs int32
	// Brokers contains each broker in the res.
	Brokers []*meta.Broker
	// ClusterID contains the cluster ID that responding broker belongs to.
	ClusterID *string
	// ControllerID contains the ID of the controller broker.
	ControllerID int32
	// Topics contains each topic in the res.
	Topics []*meta.TopicMetadata
}

func (r *MetadataResponse) Decode(pd decoder.PacketDecoder) (err error) {
	// 4个字节，暂不知晓含义
	if r.ThrottleTimeMs, err = pd.GetInt32(); err != nil {
		return err
	}

	// 实例信息
	n, err := pd.GetArrayLength()
	if err != nil {
		return err
	}
	r.Brokers = make([]*meta.Broker, n)
	for i := 0; i < n; i++ {
		r.Brokers[i] = new(meta.Broker)
		err = r.Brokers[i].Decode(pd)
		if err != nil {
			return err
		}
	}

	// 集群ID
	if r.ClusterID, err = pd.GetNullableString(); err != nil {
		return err
	}

	// 控制器ID
	if r.ControllerID, err = pd.GetInt32(); err != nil {
		return err
	}

	// 主题信息
	if numTopics, err := pd.GetArrayLength(); err != nil {
		return err
	} else {
		r.Topics = make([]*meta.TopicMetadata, numTopics)
		for i := 0; i < numTopics; i++ {
			block := &meta.TopicMetadata{}
			if err := block.Decode(pd); err != nil {
				return err
			}
			r.Topics[i] = block
		}
	}

	return nil
}

func (r *MetadataResponse) Encode(pe encoder.PacketEncoder) (err error) {
	pe.PutInt32(r.ThrottleTimeMs)

	if err := pe.PutArrayLength(len(r.Brokers)); err != nil {
		return err
	}
	for _, broker := range r.Brokers {
		err = broker.Encode(pe)
		if err != nil {
			return err
		}
	}

	if err := pe.PutNullableString(r.ClusterID); err != nil {
		return err
	}

	pe.PutInt32(r.ControllerID)

	if err := pe.PutArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for _, block := range r.Topics {
		if err := block.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (r *MetadataResponse) APIKey() int16 {
	return 3
}

func (r *MetadataResponse) APIVersion() int16 {
	return 0
}
