package request

import (
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

type InitProducerIDRequest struct {
	TransactionalID    *string
	TransactionTimeout time.Duration
	ProducerID         int64
	ProducerEpoch      int16
}

func (i *InitProducerIDRequest) Encode(pe encoder.PacketEncoder) error {
	// 事务
	if err := pe.PutNullableString(i.TransactionalID); err != nil {
		return err
	}

	pe.PutInt32(int32(i.TransactionTimeout / time.Millisecond))
	return nil
}

func (i *InitProducerIDRequest) Decode(pd decoder.PacketDecoder) (err error) {
	if i.TransactionalID, err = pd.GetNullableString(); err != nil {
		return err
	}
	timeout, err := pd.GetInt32()
	if err != nil {
		return err
	}
	i.TransactionTimeout = time.Duration(timeout) * time.Millisecond
	return nil
}

func (i *InitProducerIDRequest) APIKey() int16 {
	return 22
}

func (i *InitProducerIDRequest) APIVersion() int16 {
	return 0
}
