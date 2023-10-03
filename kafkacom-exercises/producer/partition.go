package producer

import (
	"hash/fnv"
	"sort"
)

// Partition 分区计算器
type Partition struct {
	Balance    map[string]int
	Partitions map[string][]int32
	Producer   *Instance
}

// NewPartition 计算分页器
func NewPartition(producer *Instance) *Partition {
	partition := &Partition{
		Balance:  make(map[string]int),
		Producer: producer,
	}

	// 加全局读锁
	partition.initPartitions()
	return partition
}

// Partition 计算分区
func (p *Partition) Partition(topicName string, key *string) int32 {
	// 加全局读锁
	p.Producer.GlobalLock.RLock()
	defer p.Producer.GlobalLock.RUnlock()
	return p.internalPartition(topicName, key)
}

func (p *Partition) internalPartition(topicName string, key *string) int32 {
	partitions, ok := p.Partitions[topicName]
	if !ok {
		return -1
	}

	// 轮序
	if key == nil {
		balance := p.Balance[topicName]
		if balance > len(partitions) {
			balance = len(partitions)
		}
		p.Balance[topicName] = (balance + 1) % len(partitions)
		return partitions[balance]
	}

	// hash
	hash := fnv.New32()
	if _, err := hash.Write([]byte(*key)); err != nil {
		return -1
	}
	return partitions[int(hash.Sum32())%len(partitions)]
}

// Reset 重置
func (p *Partition) Reset() {
	p.initPartitions()
}

// initPartitions 初始化分区内容
func (p *Partition) initPartitions() {
	partitions := make(map[string][]int32)
	for topicName, ps := range p.Producer.Client.Metadata {
		partitions[topicName] = make([]int32, 0, len(ps))
		for index := range ps {
			partitions[topicName] = append(partitions[topicName], index)
		}
		sort.Slice(partitions[topicName], func(i, j int) bool {
			return partitions[topicName][i] < partitions[topicName][j]
		})
	}
	p.Partitions = partitions
}
