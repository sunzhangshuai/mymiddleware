package consumer

import (
	"sync"
)

// Assigner 分配器
type Assigner struct {
	parent              *Member
	partitions          map[string][]int32
	topicPartitionState map[string]map[int32]*OffsetManager
	retry               int

	sync.Mutex
}

// NewAssigner 初始化分配器
func NewAssigner(m *Member) (*Assigner, error) {
	a := &Assigner{
		parent:              m,
		partitions:          make(map[string][]int32),
		topicPartitionState: make(map[string]map[int32]*OffsetManager),
		//retry:               m.parent.Config.Consumer.Rebalance.Retry,
		Mutex: sync.Mutex{},
	}
	return a, nil
}

//// Init 支持重试，这个函数同一时间只会有一次调用，无需加锁
//func (a *Assigner) Init() error {
//	c, err := a.parent.parent.Coordinator.Coordinator()
//	if err != nil {
//		return a.retryInit(err)
//	}
//
//	// 没有 memberID 的话，先获取 memberID

//}
//
//// retryInit 初始化重试
//func (a *Assigner) retryInit(err error) error {
//	if a.retry == 0 {
//		return err
//	}
//	a.retry--
//	return a.Init()
//}
