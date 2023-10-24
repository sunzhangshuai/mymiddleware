package consumer

import (
	"fmt"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network/method"
	"sync"
	"time"
)

// Committer 提交器
type Committer struct {
	group  *Group
	member *Member

	interval   time.Duration // 提交间隔
	curTime    *time.Time    // 当前执行时间
	nextTime   *time.Time    // 下一次执行时间
	autoCommit bool          // 自动提交
}

// NewCommitter 提交器
func NewCommitter(g *Group, m *Member) *Committer {
	return &Committer{
		group:  g,
		member: m,

		interval:   3 * time.Second,
		autoCommit: false,
	}
}

// StartAutoCommit 开启自动提交
func (c *Committer) StartAutoCommit() {
	c.autoCommit = true
}

// StopAutoCommit 停止自动提交
func (c *Committer) StopAutoCommit() {
	c.autoCommit = false
}

// AsyncCommitOffsets 提交偏移量
func (c *Committer) AsyncCommitOffsets() {
	t := time.Now()
	// 不能开始，或者自动提交关闭
	if c.nextTime == nil || !c.autoCommit {
		return
	}
	if t.Sub(*c.nextTime) >= 0 {
		c.curTime = c.nextTime
		c.nextTime = nil
		go c.commit(*c.curTime)
	}
}

// SyncCommitOffsets 同步提交偏移量
func (c *Committer) SyncCommitOffsets() {
	t := time.Now()
	c.curTime = &t
	if len(c.member.subscription.AssignedPartitions()) > 0 {
		c.commit(*c.curTime)
	} else {
		t = c.curTime.Add(c.interval)
		c.nextTime = &t
	}
}

// FetchCommittedOffsets 获取提交偏移量
func (c *Committer) FetchCommittedOffsets() map[meta.TopicPartition]*method.OffsetBlock {
	partitions := c.member.subscription.AssignedPartitions()
	res, err := method.OffsetFetch(c.member.coordinator.client(), c.group.GroupID, partitions.GetMap())
	for err != nil {
		res, err = method.OffsetFetch(c.member.coordinator.client(), c.group.GroupID, partitions.GetMap())
	}
	result := make(map[meta.TopicPartition]*method.OffsetBlock)
	for _, block := range res {
		tp := meta.TopicPartition{Topic: block.Topic, Partition: block.Partition}
		result[tp] = block
	}
	return result
}

// commit 提交
func (c *Committer) commit(t time.Time) {
	partitions := c.member.subscription.NeedCommitter()
	if len(partitions) == 0 {
		return
	}
	c.member.Log("开始提交offset=====================")
	// 提交
	blocks := make(map[string]map[int]method.OffsetBlocks)
	for i, tp := range partitions {
		// 不要 -1的
		if c.member.subscription.GetCommitted(tp) <= 0 {
			continue
		}

		if _, ok := blocks[tp.Topic]; !ok {
			blocks[tp.Topic] = make(map[int]method.OffsetBlocks)
		}
		idx := i / 3
		if _, ok := blocks[tp.Topic][idx]; !ok {
			blocks[tp.Topic][idx] = make(method.OffsetBlocks, 0, 3)
		}

		blocks[tp.Topic][idx] = append(blocks[tp.Topic][idx], &method.OffsetBlock{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    c.member.subscription.GetCommitted(tp),
			Metadata:  c.member.subscription.GetMetadata(tp),
		})
	}

	wg := sync.WaitGroup{}
	for topic, bs := range blocks {
		for i, offsetBlocks := range bs {
			wg.Add(1)
			go func(i int, topic string, offsetBlocks method.OffsetBlocks) {
				defer wg.Done()
				err := method.OffsetCommit(c.member.coordinator.client(), c.group.GroupID, c.member.memberID, c.member.generationID, offsetBlocks)
				if err != nil {
					fmt.Printf("[%s]提交offset 提交失败\n", time.Now().Format("04:05.000"))
				} else {
					//a, _ := json.Marshal(offsetBlocks)
					//fmt.Printf("[%s][%s][%d]提交offset 提交成功：%s\n", time.Now().Format("04:05.000"), topic, i, string(a))
				}
			}(i, topic, offsetBlocks)
		}
	}
	wg.Wait()

	// 添加下一次任务
	if c.curTime.Sub(t) == 0 {
		t = time.Now().Add(c.interval)
		c.nextTime = &t
	}
}
