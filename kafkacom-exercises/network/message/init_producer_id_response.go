package message

import (
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

type InitProducerIDResponse struct {
	ThrottleTime  time.Duration
	Err           util.KError
	ProducerID    int64
	ProducerEpoch int16
}

func (i *InitProducerIDResponse) Encode(pe encoder.PacketEncoder) error {
	pe.PutInt32(int32(i.ThrottleTime / time.Millisecond))
	pe.PutInt16(int16(i.Err))
	pe.PutInt64(i.ProducerID)
	pe.PutInt16(i.ProducerEpoch)
	pe.PutEmptyTaggedFieldArray()

	return nil
}

func (i *InitProducerIDResponse) Decode(pd decoder.PacketDecoder) (err error) {
	throttleTime, err := pd.GetInt32()
	if err != nil {
		return err
	}
	i.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	kerr, err := pd.GetInt16()
	if err != nil {
		return err
	}
	i.Err = util.KError(kerr)

	if i.ProducerID, err = pd.GetInt64(); err != nil {
		return err
	}

	if i.ProducerEpoch, err = pd.GetInt16(); err != nil {
		return err
	}

	if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
		return err
	}

	return nil
}

func (i *InitProducerIDResponse) APIKey() int16 {
	return 22
}

func (i *InitProducerIDResponse) APIVersion() int16 {
	return 0
}
