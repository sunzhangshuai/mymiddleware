package producer

import (
	"kafkacom-exercises/client"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network/method"
	"sync"
	"time"
)

const (
	RecordAccumulatorTickerTime  = 1 * time.Second
	RecordAccumulatorQueueMaxNum = 50
)

// Instance 生产者
type Instance struct {
	Client *client.Client
	Config *client.Config

	ProducerID    int64 // 生产者ID
	ProducerEpoch int16 // 生产者纪元

	Partition         *Partition         // 分区器
	RecordAccumulator *RecordAccumulator // 消息收集器
	Sender            *Sender            // 消息发送器
	Network           *Network           // 网络连接对象
	Counter           *Counter           // 计数器

	GlobalLock sync.RWMutex // 全局锁，读写锁，更新时上写锁，其他情况读锁
	MetaSign   chan struct{}
}

// NewProducer 创建生产者
func NewProducer(addr string) (*Instance, error) {
	config := client.NewConfig()
	cli, err := client.NewClient(addr, config)
	if err != nil {
		return nil, err
	}

	// 获取生产者信息
	initResult, err := method.InitProducer(cli.AnyBroker())
	if err != nil {
		return nil, err
	}

	// 初始化 p
	p := &Instance{
		Client:        cli,
		Config:        config,
		ProducerID:    initResult.ProducerID,
		ProducerEpoch: initResult.ProducerEpoch,
		MetaSign:      make(chan struct{}, 10),
	}

	// 初始化分区器
	p.Partition = NewPartition(p)
	// 初始化消息收集器
	p.RecordAccumulator = NewRecordAccumulator(p)
	// 初始化消息发送器
	p.Sender = NewSender(p)
	// 初始化网络连接对象
	p.Network = NewNetwork(p)

	p.Counter = NewCounter()

	// 监听元数据变更
	cli.AppendObserver("test", p.MetaSign)

	// 异步处理
	go p.handler()
	return p, nil
}

// SendMessage 同步发送消息
func (p *Instance) SendMessage(topic string, value string, key *string) (int32, int64, error) {
	m := &meta.Message{
		Key:       key,
		Value:     value,
		Topic:     topic,
		Timestamp: time.Now(),
		SyncSign:  make(chan *meta.MessageResponse, 1),
	}
	p.send(m)

	// 同步等待
	res := <-m.SyncSign
	return res.Partition, res.Offset, res.Err
}

// SendAsyncMessage 异步发送消息
func (p *Instance) SendAsyncMessage(topic string, value string, key *string,
	callback func(*meta.MessageResponse)) {
	m := &meta.Message{
		Key:       key,
		Value:     value,
		Topic:     topic,
		Timestamp: time.Now(),
		Callback:  callback,
	}
	p.send(m)
	return
}

// send 发送消息
func (p *Instance) send(message *meta.Message) {
	// 1. 计算分区
	message.PartitionNum = p.Partition.Partition(message.Topic, message.Key)
	message.SequenceNumber = p.Counter.Get(message.Topic, message.PartitionNum)
	// 2. 放到记录收集器
	p.RecordAccumulator.Accumulator(message)
}

// handler 处理
func (p *Instance) handler() {
	ticker := time.NewTicker(RecordAccumulatorTickerTime)
	for {
		select {
		case <-ticker.C: // 定时将记录收集器的数据传递给发送线程。
			p.RecordAccumulator.Send()
		case <-p.MetaSign: // 元数据变更
			func() {
				// 全局写锁
				p.GlobalLock.Lock()
				defer p.GlobalLock.Unlock()

				// 更新分区
				p.Partition.Reset()

				// 更新记录收集器
				p.RecordAccumulator.Reset()

				// 更新连接对象
				p.Network.Reset()
			}()
		}
	}
}
