package consumer

import (
	"encoding/json"
	"fmt"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network/method"
	"sync"
	"time"
)

// Output 输出
type Output struct {
	size    int64
	data    chan<- []*meta.Message
	timeout time.Duration

	reFetch bool
}

// Fetcher 拉取器
type Fetcher struct {
	group  *Group
	member *Member

	availableTopicPartitions TopicPartitions                         // 可用的分区
	messages                 map[meta.TopicPartition][]*meta.Message // 消息存储

	output   chan *Output // 输出信号
	reOutput chan *Output
	fetch    chan struct{}                                    // 抓取信号
	save     chan map[meta.TopicPartition]*method.OffsetBlock // 保存信号
	close    chan struct{}

	waitOutput *Output // 等待输出
	isFetching bool    // 是否正在拉取中
}

// NewFetcher 获取拉取器
func NewFetcher(g *Group, m *Member) *Fetcher {
	f := &Fetcher{
		group:  g,
		member: m,

		availableTopicPartitions: make(TopicPartitions, 0),
		messages:                 make(map[meta.TopicPartition][]*meta.Message),
		output:                   make(chan *Output, 10),
		reOutput:                 make(chan *Output, 10),
		fetch:                    make(chan struct{}, 10),
		save:                     make(chan map[meta.TopicPartition]*method.OffsetBlock, 10),
		close:                    make(chan struct{}, 10),
	}
	go f.handler()
	return f
}

// FetchMessages 拉取消息
func (f *Fetcher) FetchMessages(maxMessage int64, timeout time.Duration) []*meta.Message {
	data := make(chan []*meta.Message)
	f.output <- &Output{
		size:    maxMessage,
		data:    data,
		timeout: timeout,
	}
	messages := <-data
	close(data)
	return messages
}

// Close 关闭
func (f *Fetcher) Close() {
	f.close <- struct{}{}
}

func (f *Fetcher) handler() {
	t := time.NewTimer(time.Hour)
	for {
		select {
		case output := <-f.output:
			if len(f.availableTopicPartitions) == 0 {
				// 如果已经有等待了，这次拿不到任何东西，只允许同时存在一个获取
				if f.waitOutput != nil {
					output.data <- nil
					continue
				}

				// 设置等待输出
				f.waitOutput = output
				// 重新设置超时时间，规定时间内拿不到会返回空数据
				t = time.NewTimer(output.timeout)
				// 发出拉取信号
				f.fetch <- struct{}{}
				continue
			}

			// 获取结果，如果有分区空了，发送抓取请求
			messages, overflow := f.outputHandler(output.size)

			if overflow {
				f.fetch <- struct{}{}
			}
			output.data <- messages
		case output := <-f.reOutput:
			// 获取结果，如果有分区空了，发送抓取请求
			messages, overflow := f.outputHandler(output.size)
			if overflow {
				f.fetch <- struct{}{}
			}
			output.data <- messages
		case <-t.C: // 超时后。如果有等待的输出，直接返回空结果
			if f.waitOutput == nil {
				continue
			}
			f.waitOutput.data <- nil
			f.waitOutput = nil
		case <-f.fetch:
			// 如果正在拉取，忽略消息
			if f.isFetching {
				continue
			}

			// 进行异步拉取
			//fmt.Printf("[%s]开始获取消息\n", time.Now().Format("04:05.000"))
			go func() {
				// 将数据放到结果内
				f.isFetching = true
				result := f.fetchHandler()
				f.isFetching = false
				f.save <- result
			}()
		case data := <-f.save:
			if len(data) > 0 {
				//fmt.Printf("[%s]获取到了消息\n", time.Now().Format("04:05.000"))
			} else {
				//fmt.Printf("[%s]没获取到消息\n", time.Now().Format("04:05.000"))
			}
			for tp, messages := range data {
				// 如果该分区有结果，则丢弃
				if _, ok := f.messages[tp]; ok {
					continue
				}
				// 更新数据
				f.availableTopicPartitions = append(f.availableTopicPartitions, tp)
				f.messages[tp] = messages.Messages
				if messages.Offset > 10000 {
					a, _ := json.Marshal(messages)
					fmt.Println(3333, string(a))
				}
				// 更新拉取offset
				f.member.subscription.Positioned(tp, messages.Offset)
			}
			// 如果有等待的输出，将等待重新扔到输出中去
			if f.waitOutput == nil {
				continue
			}
			f.reOutput <- f.waitOutput
			f.waitOutput = nil
		case <-f.close:
			close(f.output)
			close(f.fetch)
			close(f.save)
			if f.waitOutput != nil {
				f.waitOutput.data <- nil
			}
			return
		}
	}
}

// fetchHandler 拉取消息并保存结果
func (f *Fetcher) fetchHandler() map[meta.TopicPartition]*method.OffsetBlock {
	tps := f.member.subscription.AssignedPartitions()

	// 收集请求
	requests := make(map[int32]method.OffsetBlocks)
	for _, tp := range tps {
		// 只获取没有数据的分区
		if len(f.messages[tp]) != 0 {
			continue
		}
		brokerID := f.group.Client.Metadata[tp.Topic][tp.Partition].Leader
		if _, ok := requests[brokerID]; !ok {
			requests[brokerID] = make(method.OffsetBlocks, 0)
		}
		requests[brokerID] = append(requests[brokerID], &method.OffsetBlock{
			Topic:       tp.Topic,
			Partition:   tp.Partition,
			Offset:      f.member.subscription.GetPosition(tp),
			Metadata:    f.member.subscription.GetMetadata(tp),
			LeaderEpoch: f.group.Client.Metadata[tp.Topic][tp.Partition].LeaderEpoch,
		})
	}

	// 开始并发请求
	wg := sync.WaitGroup{}
	wg.Add(len(requests))

	mu := sync.Mutex{}
	result := make(map[meta.TopicPartition]*method.OffsetBlock)
	for brokerID, r := range requests {
		go func(brokerID int32, offsets method.OffsetBlocks) {
			defer wg.Done()

			res, err := method.FetchMessages(f.group.Client.Brokers[brokerID], offsets)
			if err != nil {
				a, _ := json.Marshal(offsets)
				fmt.Printf("[%s]获取出错了[%s]：%s\n", time.Now().Format("04:05.000"), err, string(a))

			}
			mu.Lock()
			defer mu.Unlock()
			for _, block := range res {
				tp := meta.TopicPartition{Topic: block.Topic, Partition: block.Partition}
				if len(block.Messages) == 0 {
					continue
				}
				result[tp] = block
			}
		}(brokerID, r)
	}

	// 等待结束
	wg.Wait()
	f.isFetching = false
	return result
}

// outputHandler 输出处理
func (f *Fetcher) outputHandler(maxSize int64) ([]*meta.Message, bool) {
	var result []*meta.Message
	usePartitions := 0
	for _, tp := range f.availableTopicPartitions {
		if int64(len(f.messages[tp])) > maxSize {
			// 只扔前面的结果
			result = append(result, f.messages[tp][:maxSize]...)
			f.messages[tp] = f.messages[tp][maxSize:]
		} else {
			usePartitions++
			result = append(result, f.messages[tp]...)
			delete(f.messages, tp)
		}
	}
	// 去掉已经用完的分区队列
	f.availableTopicPartitions = f.availableTopicPartitions[usePartitions:]
	return result, usePartitions > 0
}
