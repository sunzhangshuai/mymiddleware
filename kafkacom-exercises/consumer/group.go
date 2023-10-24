package consumer

import (
	"kafkacom-exercises/client"
	"sync"
)

// Group 消费者实例
type Group struct {
	GroupID string

	Client *client.Client
	Config *client.Config

	// 协调者锁
	CoordinatorID   *int32
	coordinatorLock sync.Mutex
}

// NewConsumerGroup 消费者，订阅模式
func NewConsumerGroup(addr string, groupID string) (*Group, error) {
	config := client.NewConfig()
	cli, err := client.NewClient(addr, config)
	if err != nil {
		return nil, err
	}

	// 初始化 group
	return &Group{
		GroupID:         groupID,
		Client:          cli,
		Config:          config,
		coordinatorLock: sync.Mutex{},
	}, nil
}

// NewMember 获取新的消费者
func (g *Group) NewMember(name string, topics []string) *Member {
	return NewMember(g, name, topics)
}
