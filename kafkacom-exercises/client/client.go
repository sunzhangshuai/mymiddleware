package client

import (
	"fmt"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message"
	"kafkacom-exercises/network/method"
	"strings"
	"sync"
	"time"
)

// Client kafka客户端
type Client struct {
	Config *Config

	Brokers   map[int32]*meta.Broker                       // maps broker ids to Brokers
	Metadata  map[string]map[int32]*meta.PartitionMetadata // maps topics to partition ids to Metadata
	ClusterID *string

	// 监听者
	Observers map[string]chan struct{}

	sync.RWMutex
}

// NewClient kafka客户端
func NewClient(addr string, config *Config) (*Client, error) {
	cli := &Client{
		Brokers: map[int32]*meta.Broker{-1: {
			Addr:     addr,
			ClientID: config.ClientID,
		}},
		Observers: make(map[string]chan struct{}),
		Config:    config,
	}
	// 首次获取元数据
	cli.Brokers[-1].Open()
	if err := method.GetMetaDataSync(cli.AnyBroker(), cli.updateConfig); err != nil {
		return nil, err
	}

	// 定时更新元数据
	go func() {
		cli.flushMetaDate()
	}()

	return cli, nil
}

// AnyBroker 随机节点
func (c *Client) AnyBroker() *meta.Broker {
	for _, broker := range c.Brokers {
		return broker
	}
	return nil
}

// AppendObserver 监听者
func (c *Client) AppendObserver(key string, observer chan struct{}) {
	c.Observers[key] = observer
}

// updateConfig 更新配置
func (c *Client) updateConfig(response *network.Response) error {
	c.Lock()
	defer c.Unlock()

	body := response.ProtocolBody.(*message.MetadataResponse)
	// 更新broker
	c.updateBroker(body.Brokers)
	c.updateMetadata(body.Topics)
	c.ClusterID = body.ClusterID

	// 通知监听者
	for _, observer := range c.Observers {
		observer <- struct{}{}
	}
	return nil
}

// updateBroker 更新节点
func (c *Client) updateBroker(brokers []*meta.Broker) {
	current := make(map[int32]bool, len(brokers))

	// 新增的加入
	for _, broker := range brokers {
		current[broker.ID] = true
		if _, ok := c.Brokers[broker.ID]; !ok {
			c.Brokers[broker.ID] = broker
			broker.Open()
		}
	}

	// 掉线的清除
	for id, broker := range c.Brokers {
		if _, ok := current[id]; !ok {
			broker.Close()
			delete(c.Brokers, id)
		}
	}
}

// updateMetadata 更新元数据
func (c *Client) updateMetadata(metadata []*meta.TopicMetadata) {
	current := make(map[string]map[int32]*meta.PartitionMetadata, len(metadata))

	for _, topic := range metadata {
		current[topic.Name] = make(map[int32]*meta.PartitionMetadata, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			current[topic.Name][partition.ID] = partition
		}
	}
	c.Metadata = current
}

// flushMetaDate 刷新元数据
func (c *Client) flushMetaDate() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := method.GetMetaData(c.AnyBroker(), c.updateConfig); err != nil {
				fmt.Printf("更新元数据出错了: %+v\n", err)
			}
			// fmt.Printf("更新元数据成功：%s\n", c)
		}
	}
}

func (c *Client) String() string {
	bs := make([]string, 0, len(c.Brokers))
	for _, b := range c.Brokers {
		bs = append(bs, fmt.Sprintf("%d|%s", b.ID, b.Addr))
	}
	ts := make([]string, 0, len(c.Metadata))
	for name, t := range c.Metadata {
		ps := make([]string, 0, len(t))
		for _, p := range t {
			ps = append(ps, fmt.Sprintf("%d|%d|%v", p.ID, p.Leader, p.Isr))
		}
		ts = append(ts, fmt.Sprintf("%s=================\n%s", name, strings.Join(ps, "\n")))
	}
	return fmt.Sprintf("broker有：%s\n: topic有：%s\n", strings.Join(bs, ","), strings.Join(ts, "\n"))
}
