package consumer

import (
	"encoding/json"
	"kafkacom-exercises/consumer/balance"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network/method"
	"kafkacom-exercises/util"
)

// Coordinator 协调者
type Coordinator struct {
	member *Member // 所属的消费者
	group  *Group  // 组

	rejoinNeeded bool ///需要重新加入消费组
	dead         bool // 是否挂了
	coordinator  *meta.Broker
}

// NewCoordinator 获取协调者
func NewCoordinator(g *Group, m *Member) *Coordinator {
	return &Coordinator{
		member:       m,
		group:        g,
		rejoinNeeded: true,
		dead:         false,
	}
}

// Dead 协调者挂了
func (c *Coordinator) Dead() {
	c.dead = true
}

// EnsureCoordinatorKnown 确保协调者已知
func (c *Coordinator) EnsureCoordinatorKnown() {
	c.group.coordinatorLock.Lock()
	defer c.group.coordinatorLock.Unlock()

	// 如果存在的话，直接返回
	if c.coordinator != nil && !c.dead && c.group.CoordinatorID != nil {
		if _, ok := c.group.Client.Brokers[*c.group.CoordinatorID]; ok {
			return
		}
	}

	// 不存在重新获取
	b, err := method.FindCoordinator(c.group.Client.LeastLoadedBroker(), c.group.GroupID)
	for err != nil {
		b, err = method.FindCoordinator(c.group.Client.LeastLoadedBroker(), c.group.GroupID)
	}
	c.group.CoordinatorID = &b.ID
	// 需要一个新的协调者连接
	b.Open()
	c.coordinator = b
	c.dead = false
}

// EnsurePartitionAssignment 确保协调者为消费者分配了分区
func (c *Coordinator) EnsurePartitionAssignment() {
	// 判断是否需要分配分区
	if !c.member.subscription.PartitionsAssignmentNeeded() {
		return
	}
	c.member.Log("开始分配分区===========")
	// 加入前准备工作，比如提交偏移量
	c.onJoinPrepare()

	// 加入消费组
	tps, memberID, generationID := c.joinGroup(c.member.memberID, c.member.generationID)
	a, _ := json.Marshal(tps)
	c.member.Log("分配分区完成:%s", string(a))

	// 加入完成
	c.onJoinComplete(memberID, generationID, tps)

	return
}

// RefreshCommittedOffsets 如果需要更新偏移量，则刷新
func (c *Coordinator) RefreshCommittedOffsets() {
	// 获取offset
	offsets := c.member.committer.FetchCommittedOffsets()

	// 抓取到的offset
	c.member.Log("协调器存储的offset开始==============")
	for partition, block := range offsets {
		c.member.Log("%s: %d", partition, block.Offset)
	}
	c.member.Log("协调器存储的offset结束==============")

	// 更新每个分区状态
	for tp, i := range offsets {
		c.member.subscription.Committed(tp, 0)
		c.member.subscription.Metadata(tp, i.Metadata)
	}

	// 提交偏移量重置完成
	c.member.subscription.CommitsRefreshed()
}

// onJoinPrepare 加入前的准备工作
func (c *Coordinator) onJoinPrepare() {
	// 保存消费进度，暂停自动提交
	c.member.memberID = ""
	c.member.committer.StopAutoCommit()
	c.member.committer.SyncCommitOffsets()

	// 需要刷新提交偏移量
	c.member.subscription.NeedRefreshCommits()
}

// onJoinComplete 加入完成
func (c *Coordinator) onJoinComplete(memberID string, generationID int32, tps TopicPartitions) {
	c.member.memberID = memberID
	c.member.generationID = generationID

	// 更新分配结果，重新分配分区完成
	c.member.subscription.AssignTopicPartitions(tps)
	c.member.subscription.ReassignmentComplete()

	// 启动提交任务
	c.member.committer.StartAutoCommit()
}

// joinGroup 加入消费组
func (c *Coordinator) joinGroup(memberID string, generationID int32) (TopicPartitions, string, int32) {
	// 获取到memberID
	if memberID == "" {
		g, err := method.JoinGroup(c.client(), c.group.GroupID,
			c.member.memberID, c.member.topics, c.group.Client.Config.Consumer.Session.Timeout,
			c.group.Client.Config.Consumer.Rebalance.Timeout,
			c.group.Client.Config.Consumer.Rebalance.Strategy.Name())
		if g.Err == util.ErrMemberIdRequired || err != nil {
			return c.joinGroup(g.MemberID, generationID)
		}
	}

	// 加入消费组，拿到generationID
	g, err := method.JoinGroup(c.client(), c.group.GroupID,
		memberID, c.member.topics, c.group.Client.Config.Consumer.Session.Timeout,
		c.group.Client.Config.Consumer.Rebalance.Timeout,
		c.group.Client.Config.Consumer.Rebalance.Strategy.Name())
	if err != nil || g.Err == util.ErrMemberIdRequired {
		return c.joinGroup(memberID, g.GenerationId)
	}
	generationID = g.GenerationId

	// 同步组
	var plan balance.StrategyPlan
	var members map[string]*meta.ConsumerGroupMember
	// 主消费者需要进行分区分配
	if g.LeaderId == memberID {
		members = make(map[string]*meta.ConsumerGroupMember, len(g.Members))
		for _, member := range g.Members {
			members[member.MemberId] = member.Metadata
		}
		topicPartitions := make(map[string][]int32, len(c.member.topics))
		for _, topic := range c.member.topics {
			if ps := c.group.Client.Partitions(topic); ps != nil {
				topicPartitions[topic] = ps
			}
		}
		a, _ := json.Marshal(g.Members)
		c.member.Log("消费组成员如下：%s", string(a))

		if plan, err = c.group.Client.Config.Consumer.
			Rebalance.Strategy.Plan(members, topicPartitions); err != nil {
			return c.joinGroup(memberID, generationID)
		}
	}
	assignment, err := method.SyncGroup(c.client(), c.group.GroupID, memberID, generationID, plan)
	if err != nil || len(assignment.Topics) == 0 || assignment.Version == -1 {
		return c.joinGroup("", 0)
	}
	res := make(TopicPartitions, 0)
	res.SetMap(assignment.Topics)
	return res, memberID, generationID
}

// client 客户端
func (c *Coordinator) client() *meta.Broker {
	c.EnsureCoordinatorKnown()
	return c.coordinator
}
