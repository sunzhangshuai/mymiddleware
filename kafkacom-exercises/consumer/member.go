package consumer

import (
	"encoding/json"
	"fmt"
	"kafkacom-exercises/meta"
	"time"
)

// Member 消费者
type Member struct {
	group  *Group   // 所属组
	topics []string // topics
	name   string   // 用户名称

	coordinator      *Coordinator      // 协调者
	subscription     *Subscription     // 订阅信息
	committer        *Committer        // 提交器
	fetcher          *Fetcher          // 拉取器
	heartbeatManager *HeartbeatManager // 心跳管理器

	memberID     string // memberID
	generationID int32  // 服务器提供的唯一ID
}

// NewMember 获取新的消费者
func NewMember(group *Group, name string, topics []string) *Member {
	m := &Member{
		group:  group,
		topics: topics,
		name:   name,
	}
	m.coordinator = NewCoordinator(group, m)
	m.subscription = NewSubscription(group, m)
	m.committer = NewCommitter(group, m)
	m.fetcher = NewFetcher(group, m)
	m.heartbeatManager = NewHeartbeatManager(group, m)
	return m
}

func (m *Member) Consumer(callback func(*meta.Message)) {
	for {
		// 获取结果
		messages := m.pollOnce(2 * time.Second)

		// 处理结果
		for _, message := range messages {
			// 如果需要重新分配分区，直接结束
			if m.subscription.PartitionsAssignmentNeeded() {
				break
			}

			// 提前拿到变量，避免修改
			tp := meta.TopicPartition{
				Topic:     message.Topic,
				Partition: message.PartitionNum,
			}
			offset := message.Offset

			// 处理消息
			callback(message)

			// 更新提交偏移量
			m.subscription.Committed(tp, offset)
		}
	}
}

// updateFetchPositions 更新拉取偏移量
func (m *Member) updateFetchPositions() {
	// 更新提交偏移量
	m.coordinator.RefreshCommittedOffsets()

	// 更新分区的拉取偏移量
	for _, tp := range m.subscription.AssignedPartitions() {
		// 将提交偏移量改为拉取偏移量
		committed := m.subscription.GetCommitted(tp)
		if committed == -1 {
			m.subscription.Seek(tp, 0)
		} else {
			m.subscription.Seek(tp, committed)
		}

	}
}

// pollOnce 轮序
func (m *Member) pollOnce(timeout time.Duration) []*meta.Message {
	start := time.Now()
	// 需要已知协调者
	m.coordinator.EnsureCoordinatorKnown()
	// 需要确保分配分区
	m.coordinator.EnsurePartitionAssignment()

	// 如果需要刷新提交偏移量的话
	if m.subscription.RefreshCommitsNeeded() {
		m.updateFetchPositions()

		// 更新拉取器
		m.fetcher.Close()
		m.fetcher = NewFetcher(m.group, m)
	}

	// 定时任务
	m.heartbeatManager.Run()
	m.committer.AsyncCommitOffsets()

	// 发送拉取请求
	messages := m.fetcher.FetchMessages(m.group.Client.Config.Consumer.Client.MaxLen, timeout)
	if sub := time.Since(start); len(messages) == 0 && sub < timeout {
		time.Sleep(timeout - sub)
	}
	return messages
}

func (m *Member) Log(format string, obj ...interface{}) string {
	str := fmt.Sprintf(format, obj...)
	fmt.Printf("[%s][%s]%s\n", m.name, time.Now().Format("04:05.000"), str)
	b, _ := json.MarshalIndent(obj, "", "  ")
	return string(b)
}
