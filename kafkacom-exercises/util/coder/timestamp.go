package coder

import (
	"fmt"
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"time"
)

type Timestamp struct {
	*time.Time
}

func (t Timestamp) Encode(pe encoder.PacketEncoder) error {
	timestamp := int64(-1)

	if !t.Before(time.Unix(0, 0)) {
		timestamp = t.UnixNano() / int64(time.Millisecond)
	} else if !t.IsZero() {
		return util.PacketEncodingError{Info: fmt.Sprintf("invalid timestamp (%v)", t)}
	}

	pe.PutInt64(timestamp)
	return nil
}

func (t Timestamp) Decode(pd decoder.PacketDecoder) error {
	millis, err := pd.GetInt64()
	if err != nil {
		return err
	}

	// negative timestamps are invalid, in these cases we should return
	// a zero time
	timestamp := time.Time{}
	if millis >= 0 {
		timestamp = time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
	}

	*t.Time = timestamp
	return nil
}
