package response

import (
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

type OffsetCommitResponse struct {
	Errors map[string]map[int32]util.KError
}

func (r *OffsetCommitResponse) Encode(pe encoder.PacketEncoder) error {
	if err := pe.PutArrayLength(len(r.Errors)); err != nil {
		return err
	}
	for topic, partitions := range r.Errors {
		if err := pe.PutString(topic); err != nil {
			return err
		}
		if err := pe.PutArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, kerror := range partitions {
			pe.PutInt32(partition)
			pe.PutInt16(int16(kerror))
		}
	}
	return nil
}

func (r *OffsetCommitResponse) Decode(pd decoder.PacketDecoder) error {
	var err error

	numTopics, err := pd.GetArrayLength()
	if err != nil || numTopics == 0 {
		return err
	}
	r.Errors = make(map[string]map[int32]util.KError, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}
		numErrors, err := pd.GetArrayLength()
		if err != nil {
			return err
		}
		r.Errors[name] = make(map[int32]util.KError, numErrors)

		for j := 0; j < numErrors; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			tmp, err := pd.GetInt16()
			if err != nil {
				return err
			}
			r.Errors[name][id] = util.KError(tmp)
		}
	}
	return nil
}

func (r *OffsetCommitResponse) APIKey() int16 {
	return 8
}

func (r *OffsetCommitResponse) APIVersion() int16 {
	return 6
}
