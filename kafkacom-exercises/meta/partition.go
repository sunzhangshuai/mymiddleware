package meta

import (
	"kafkacom-exercises/util"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
)

// PartitionMetadata 包含主题中的每个分区。
type PartitionMetadata struct {
	// Err 包含错误码，如果没有错误则为0。
	Err util.KError
	// ID 包含分区索引。
	ID int32
	// Leader 包含leader broker的ID。
	Leader int32
	// LeaderEpoch 包含此分区的前导纪元。
	LeaderEpoch int32
	// Replicas 包含承载此分区的所有节点的集合。
	Replicas []int32
	// Isr 包含与此分区的前导同步的一组节点。
	Isr []int32
	// OfflineReplicas 包含此分区的一组脱机副本。
	OfflineReplicas []int32
}

func (p *PartitionMetadata) Decode(pd decoder.PacketDecoder) (err error) {
	// 2字节的错误码
	tmp, err := pd.GetInt16()
	if err != nil {
		return err
	}
	p.Err = util.KError(tmp)

	// 分区索引
	if p.ID, err = pd.GetInt32(); err != nil {
		return err
	}

	// 主副本所在实例ID
	if p.Leader, err = pd.GetInt32(); err != nil {
		return err
	}

	// 包含此分区的前导纪元
	if p.LeaderEpoch, err = pd.GetInt32(); err != nil {
		return err
	}

	// 所有副本所在的实例集合
	if p.Replicas, err = pd.GetInt32Array(); err != nil {
		return err
	}

	// 同步副本集合
	if p.Isr, err = pd.GetInt32Array(); err != nil {
		return err
	}

	// 已下线的副本集合
	if p.OfflineReplicas, err = pd.GetInt32Array(); err != nil {
		return err
	}

	return nil
}

func (p *PartitionMetadata) Encode(pe encoder.PacketEncoder) (err error) {
	pe.PutInt16(int16(p.Err))

	pe.PutInt32(p.ID)

	pe.PutInt32(p.Leader)

	pe.PutInt32(p.LeaderEpoch)

	if err := pe.PutInt32Array(p.Replicas); err != nil {
		return err
	}

	if err := pe.PutInt32Array(p.Isr); err != nil {
		return err
	}

	if err := pe.PutInt32Array(p.OfflineReplicas); err != nil {
		return err
	}

	return nil
}
