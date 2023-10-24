package consumer

import (
	"kafkacom-exercises/network/method"
	"kafkacom-exercises/util"
	"time"
)

// HeartbeatManager 心跳
type HeartbeatManager struct {
	group  *Group
	member *Member

	timeout              time.Duration // 会话超时时间
	interval             time.Duration // 心跳间隔
	lastSessionReset     time.Time     // 上一次的会话重置时间
	lastHeartbeatSend    *time.Time    // 发送心跳请求时，记录发送时间
	lastHeartbeatReceive *time.Time    // 接收到心跳请求结果后，记录接收时间
}

// NewHeartbeatManager 心跳管理器
func NewHeartbeatManager(g *Group, m *Member) *HeartbeatManager {
	return &HeartbeatManager{
		group:  g,
		member: m,

		timeout:  g.Config.Consumer.Heartbeat.Timeout,
		interval: g.Config.Consumer.Heartbeat.Timeout,
	}
}

// TimeToNextHeartbeat 当前时间到下一次调度的时间间隔
func (h *HeartbeatManager) TimeToNextHeartbeat() time.Duration {
	now := time.Now()
	// 如果上次没有发送，立即调度
	if h.lastHeartbeatSend == nil || h.lastHeartbeatSend.Sub(h.lastSessionReset) < 0 {
		return 0
	}
	// 从上次发送心跳到现在过去了多久
	timeSinceLastHeartbeat := now.Sub(*h.lastHeartbeatSend)
	// 当前时间与上次发生心跳的差距超过了心跳间隔
	if timeSinceLastHeartbeat > h.interval {
		return 0
	} else {
		return h.interval - timeSinceLastHeartbeat
	}
}

// sessionTimeoutExpired 心跳超时
func (h *HeartbeatManager) sessionTimeoutExpired() bool {
	// 没有调度过
	if h.lastHeartbeatSend == nil {
		return false
	}
	// 已经接收了
	if h.lastHeartbeatReceive != nil && h.lastHeartbeatReceive.Sub(*h.lastHeartbeatSend) > 0 {
		return false
	}
	return time.Now().Sub(*h.lastHeartbeatSend) > h.timeout
}

// Run 执行心跳
func (h *HeartbeatManager) Run() {
	now := time.Now()
	// 超时时间内没有收到心跳应答，通知协调者挂掉了
	if h.sessionTimeoutExpired() {
		h.member.coordinator.Dead()
		// 重置时间
		h.lastSessionReset = time.Now()
		return
	}
	// 判断现在是否需要心跳
	if h.TimeToNextHeartbeat() == 0 {
		h.lastHeartbeatSend = &now
		h.member.Log("开始心跳=====================")
		go func(start time.Time) {
			// 发送心跳请求
			if err := method.Heartbeat(h.member.coordinator.client(), h.group.GroupID, h.member.memberID,
				h.member.generationID); err != nil && err != util.ErrNoError {
				if err == util.ErrRebalanceInProgress {
					h.member.Log("心跳结果返回，分区重平衡了=====================")
					h.member.subscription.NeedReassignment()
				}
				if err == util.ErrUnknownMemberId {
					h.member.Log("没找到memberID，协调器需要重置=====================")
					h.member.coordinator.Dead()
				}
			}

			// 如果任务已经更新过，就不更新最后一次返回时间了
			if start.Sub(*h.lastHeartbeatSend) != 0 {
				return
			}
			t := time.Now()
			h.lastHeartbeatReceive = &t
			h.member.Log("心跳结束=====================")
		}(now)
	}
}
