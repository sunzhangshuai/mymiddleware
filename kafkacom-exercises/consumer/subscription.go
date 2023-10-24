package consumer

import (
	"kafkacom-exercises/meta"
)

// TopicPartitions 分区列表
type TopicPartitions []meta.TopicPartition

// GetMap 转换为Map格式
func (t *TopicPartitions) GetMap() map[string][]int32 {
	result := make(map[string][]int32)
	for _, partition := range *t {
		if _, ok := result[partition.Topic]; !ok {
			result[partition.Topic] = make([]int32, 0)
		}
		result[partition.Topic] = append(result[partition.Topic], partition.Partition)
	}
	return result
}

// SetMap 设置map
func (t *TopicPartitions) SetMap(data map[string][]int32) {
	for topic, ps := range data {
		for _, num := range ps {
			*t = append(*t, meta.TopicPartition{
				Topic:     topic,
				Partition: num,
			})
		}
	}
}

// Subscription 订阅
type Subscription struct {
	group            *Group
	member           *Member
	assignment       TopicPartitions
	assignmentOffset map[meta.TopicPartition]*OffsetManager

	needPartitionsAssignment bool // 是否需要分配分区
	needRefreshCommits       bool // 是否需要拉取偏移量
}

// NewSubscription 订阅信息
func NewSubscription(g *Group, m *Member) *Subscription {
	return &Subscription{
		group:            g,
		member:           m,
		assignment:       make(TopicPartitions, 0),
		assignmentOffset: make(map[meta.TopicPartition]*OffsetManager),

		needPartitionsAssignment: true,
		needRefreshCommits:       true,
	}
}

// IsAssigned 是否分配
func (s *Subscription) IsAssigned(tp meta.TopicPartition) bool {
	return s.offset(tp) != nil
}

// AssignTopicPartitions 分配分区
func (s *Subscription) AssignTopicPartitions(data TopicPartitions) {
	s.assignment = data
	s.assignmentOffset = make(map[meta.TopicPartition]*OffsetManager)
	for _, partition := range s.assignment {
		s.assignmentOffset[partition] = NewOffsetManager()
	}
}

// NeedReassignment 需要重新分配分区
func (s *Subscription) NeedReassignment() {
	s.needPartitionsAssignment = true
}

// ReassignmentComplete 重新分配分区完成
func (s *Subscription) ReassignmentComplete() {
	s.needPartitionsAssignment = false
}

// PartitionsAssignmentNeeded 是否需要分配分区
func (s *Subscription) PartitionsAssignmentNeeded() bool {
	return s.needPartitionsAssignment
}

// NeedRefreshCommits 需要刷新提交偏移量
func (s *Subscription) NeedRefreshCommits() {
	s.needRefreshCommits = true
}

// RefreshCommitsNeeded 是否需要刷新提交偏移量
func (s *Subscription) RefreshCommitsNeeded() bool {
	return s.needRefreshCommits
}

// CommitsRefreshed 提交偏移量刷新成功
func (s *Subscription) CommitsRefreshed() {
	s.needRefreshCommits = false
}

// AssignedPartitions 已分配的分区
func (s *Subscription) AssignedPartitions() TopicPartitions {
	return s.assignment
}

// NeedCommitter 需要提交的分区
func (s *Subscription) NeedCommitter() TopicPartitions {
	result := make(TopicPartitions, 0)
	for tp, offset := range s.assignmentOffset {
		if offset.needCommit {
			result = append(result, tp)
		}
	}
	return result
}

// Committed 提交偏移量
func (s *Subscription) Committed(tp meta.TopicPartition, offset int64) {
	s.offset(tp).needCommit = true
	s.offset(tp).Committed(offset)
}

// CommittedPush 推到协调者了
func (s *Subscription) CommittedPush(tp meta.TopicPartition) {
	s.offset(tp).needCommit = false
}

// GetCommitted 获取提交偏移量
func (s *Subscription) GetCommitted(tp meta.TopicPartition) int64 {
	return s.offset(tp).committed
}

func (s *Subscription) Positioned(tp meta.TopicPartition, offset int64) {
	s.offset(tp).position = &offset
}

// GetPosition 获取拉取偏移量
func (s *Subscription) GetPosition(tp meta.TopicPartition) int64 {
	return *s.offset(tp).position
}

// Metadata 元数据
func (s *Subscription) Metadata(tp meta.TopicPartition, metadata string) {
	s.offset(tp).metadata = metadata
}

// GetMetadata 元数据
func (s *Subscription) GetMetadata(tp meta.TopicPartition) string {
	return s.offset(tp).metadata
}

// Seek 定位到分区的指定位置，更新拉取偏移量
func (s *Subscription) Seek(tp meta.TopicPartition, offset int64) {
	s.offset(tp).Seek(offset)
}

// offset 分区状态
func (s *Subscription) offset(tp meta.TopicPartition) *OffsetManager {
	return s.assignmentOffset[tp]
}
