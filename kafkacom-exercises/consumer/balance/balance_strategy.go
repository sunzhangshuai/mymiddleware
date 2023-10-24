package balance

import (
	m "kafkacom-exercises/meta"
	"math"
	"sort"
)

// StrategyPlan BalanceStrategy的结果，以`memberID->topic->partitions'映射形式分配的主题/分区。
type StrategyPlan map[string]map[string][]int32

// Add 为成员分配一个带有数字分区的主题。
func (p StrategyPlan) Add(memberID, topic string, partitions ...int32) {
	if len(partitions) == 0 {
		return
	}
	if _, ok := p[memberID]; !ok {
		p[memberID] = make(map[string][]int32, 1)
	}
	p[memberID][topic] = append(p[memberID][topic], partitions...)
}

// Strategy 用于在使用者组的成员之间平衡主题和分区
type Strategy interface {
	// Name 唯一标识策略
	Name() string

	// Plan 接受“memberID->metadata”的映射和“topic->partitions”的映射，并返回分发计划。
	Plan(members map[string]*m.ConsumerGroupMember, topics map[string][]int32) (StrategyPlan, error)

	// AssignmentData 共享分配数据，返回指定memberID的序列化分配数据。
	AssignmentData(memberID string, topics map[string][]int32, generationID int32) ([]byte, error)
}

type balanceStrategy struct {
	coreFn func(plan StrategyPlan, memberIDs []string, topic string, partitions []int32)
	name   string
}

// NewBalanceStrategyRange 平衡策略是默认的，它将分区作为范围分配给消费者组成员。这遵循：
// https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html
//
// 具有两个主题T1和T2的示例，每个主题有六个分区（0..5）和两个成员（M1、M2）：
//
//	M1: {T1: [0, 1, 2], T2: [0, 1, 2]}
//	M2: {T2: [3, 4, 5], T2: [3, 4, 5]}
func NewBalanceStrategyRange() Strategy {
	return &balanceStrategy{
		name: "range",
		coreFn: func(plan StrategyPlan, memberIDs []string, topic string, partitions []int32) {
			partitionsPerConsumer := len(partitions) / len(memberIDs)
			consumersWithExtraPartition := len(partitions) % len(memberIDs)

			sort.Strings(memberIDs)

			for i, memberID := range memberIDs {
				min := i*partitionsPerConsumer + int(math.Min(float64(consumersWithExtraPartition), float64(i)))
				extra := 0
				if i < consumersWithExtraPartition {
					extra = 1
				}
				max := min + partitionsPerConsumer + extra
				plan.Add(memberID, topic, partitions[min:max]...)
			}
		},
	}
}

// Name implements Strategy.
func (s *balanceStrategy) Name() string { return s.name }

// Plan implements Strategy.
func (s *balanceStrategy) Plan(members map[string]*m.ConsumerGroupMember,
	topics map[string][]int32) (StrategyPlan, error) {

	// Build members by topic map
	mbt := make(map[string][]string)
	for memberID, meta := range members {
		for _, topic := range meta.Topics {
			mbt[topic] = append(mbt[topic], memberID)
		}
	}

	// 进行排序和消除重复
	uniq := func(ss sort.StringSlice) []string {
		if ss.Len() < 2 {
			return ss
		}
		sort.Sort(ss)
		var i, j int
		for i = 1; i < ss.Len(); i++ {
			if ss[i] == ss[j] {
				continue
			}
			j++
			ss.Swap(i, j)
		}
		return ss[:j+1]
	}

	// 分配
	plan := make(StrategyPlan, len(members))
	for topic, memberIDs := range mbt {
		s.coreFn(plan, uniq(memberIDs), topic, topics[topic])
	}
	return plan, nil
}

// AssignmentData 简单策略不需要任何共享分配数据
func (s *balanceStrategy) AssignmentData(_ string, _ map[string][]int32, _ int32) ([]byte, error) {
	return nil, nil
}
