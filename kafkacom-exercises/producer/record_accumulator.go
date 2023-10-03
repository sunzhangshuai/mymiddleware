package producer

import (
	"github.com/eapache/queue"
	"kafkacom-exercises/meta"
	"sync"
)

// RecordAccumulator 记录收集器
type RecordAccumulator struct {
	Data     map[string]map[int32]*queue.Queue // 数据存储
	Input    chan *meta.Message                // 输入
	SendSign chan struct{}                     // 发送信号

	WaitSend       bool       // 等待发送标识，避免重复发送
	WaitSendLocker sync.Mutex // 等待发送锁，避免重复发送

	Producer *Instance
}

// NewRecordAccumulator 记录收集器
func NewRecordAccumulator(producer *Instance) *RecordAccumulator {
	accumulator := &RecordAccumulator{
		Data:     make(map[string]map[int32]*queue.Queue),
		Input:    make(chan *meta.Message, 100000),
		SendSign: make(chan struct{}, 2),
		Producer: producer,
	}
	// 启动处理器
	go accumulator.handler()
	return accumulator
}

// Accumulator 收集记录
func (r *RecordAccumulator) Accumulator(message *meta.Message) {
	r.Input <- message
}

// Send 发送
func (r *RecordAccumulator) Send() {
	r.WaitSendLocker.Lock()
	defer r.WaitSendLocker.Unlock()
	if r.WaitSend {
		return
	}
	// 否则通知发送器
	r.WaitSend = true
	r.SendSign <- struct{}{}
}

// Reset 重置
func (r *RecordAccumulator) Reset() {
	// 找到每条数据，重新计算分区，放进去
	oldData := r.Data
	r.Data = make(map[string]map[int32]*queue.Queue)
	for _, partitions := range oldData {
		for _, q := range partitions {
			for i := 0; i < q.Length(); i++ {
				for _, message := range *q.Remove().(*[]*meta.Message) {
					message.PartitionNum = r.Producer.Partition.internalPartition(message.Topic, message.Key)
					r.append(message)
				}
			}
		}
	}
}

// handler 处理
func (r *RecordAccumulator) handler() {
	for {
		func() {
			// 全局锁
			select {
			case message := <-r.Input: // 接收到消息
				r.Producer.GlobalLock.RLock()
				defer r.Producer.GlobalLock.RUnlock()
				r.append(message)
			case <-r.SendSign: // 接收到发送器信号
				r.Producer.GlobalLock.RLock()
				defer r.Producer.GlobalLock.RUnlock()
				r.toSender()
			}
		}()
	}
}

// append 追加消息
func (r *RecordAccumulator) append(message *meta.Message) {
	// 获取分区map
	topicName := message.Topic
	if _, ok := r.Data[topicName]; !ok {
		r.Data[topicName] = make(map[int32]*queue.Queue)
	}
	partitionMap := r.Data[topicName]
	// 选择分区队列
	if _, ok := partitionMap[message.PartitionNum]; !ok {
		partitionMap[message.PartitionNum] = queue.New()
	}
	batchRecords := partitionMap[message.PartitionNum]

	// 空队列先开节点
	var batchRecord *[]*meta.Message
	if batchRecords.Length() == 0 {
		br := make([]*meta.Message, 0, RecordAccumulatorQueueMaxNum)
		batchRecord = &br
		batchRecords.Add(batchRecord)
	}

	batchRecord = batchRecords.Get(batchRecords.Length() - 1).(*[]*meta.Message)
	// 队列满了，新开节点
	if len(*batchRecord) == RecordAccumulatorQueueMaxNum {
		// 满了需要发消息
		r.Send()

		br := make([]*meta.Message, 0, RecordAccumulatorQueueMaxNum)
		batchRecord = &br
		batchRecords.Add(batchRecord)
	}

	// 追加消息
	*batchRecord = append(*batchRecord, message)
}

// toSender 向 Sender 发消息
func (r *RecordAccumulator) toSender() {
	r.WaitSendLocker.Lock()
	r.WaitSend = false
	r.WaitSendLocker.Unlock()

	data := make(map[string]map[int32][]*meta.Message)

	for topicName, partitionMap := range r.Data {
		for partitionNum, batchRecords := range partitionMap {
			if batchRecords.Length() == 0 {
				continue
			}
			if _, ok := data[topicName]; !ok {
				data[topicName] = make(map[int32][]*meta.Message)
			}
			data[topicName][partitionNum] = *batchRecords.Remove().(*[]*meta.Message)
		}
	}
	if len(data) > 0 {
		r.Producer.Sender.Send(data)
	}
}
