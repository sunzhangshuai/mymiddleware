package producer

import (
	"github.com/eapache/queue"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/network"
	"kafkacom-exercises/network/message/response"
	"kafkacom-exercises/network/method"
)

// NetworkInput 发送器的输出
type NetworkInput struct {
	Input   map[string]map[int32][]*meta.Message   // 原始输出
	Request map[string]map[int32]*meta.RecordBatch // 请求内容
}

// Response 结果
type Response struct {
	brokerID int32
	result   map[string]map[int32]*response.ProduceResponseBlock
}

// Client 请求客户端
type Client struct {
	broker          *meta.Broker // 实例
	inFlightRequest *queue.Queue // 待请求队列
	isWait          bool         // 是否在等待
	isRetry         bool         // 是否需要重试
}

// Network 网络连接对象
type Network struct {
	clients  map[int32]*Client            // 客户端列表
	input    chan map[int32]*NetworkInput // 输入
	response chan *Response

	producer *Instance // 生产者
}

// NewNetwork 网络连接对象
func NewNetwork(producer *Instance) *Network {
	netwk := &Network{
		clients:  make(map[int32]*Client),
		input:    make(chan map[int32]*NetworkInput, 10),
		response: make(chan *Response, 10000),
		producer: producer,
	}
	netwk.initClient()

	go netwk.handler()
	return netwk
}

// Send 发送消息
func (n *Network) Send(inputs map[int32]*NetworkInput) {
	n.input <- inputs
}

// Reset 重置
func (n *Network) Reset() {
	oldClient := n.clients
	// 初始化客户端
	n.initClient()

	// 清掉消息重新发送
	for brokerID, client := range oldClient {
		// 如果存在就麻烦了，需要先初始化好，把retry置为true
		if _, ok := n.clients[brokerID]; ok {
			if client.isWait {
				n.clients[brokerID].isWait = true
				n.clients[brokerID].isRetry = true
			}
		}

		// 将每条消息都重新发送一遍
		for i := 0; i < client.inFlightRequest.Length(); i++ {
			request := client.inFlightRequest.Remove().(*NetworkInput)
			for _, partitions := range request.Input {
				for _, messages := range partitions {
					for _, m := range messages {
						m.PartitionNum = n.producer.Partition.internalPartition(m.Topic, m.Key)
						m.SequenceNumber = n.producer.Counter.Get(m.Topic, m.PartitionNum)
						n.producer.RecordAccumulator.Accumulator(m)
					}
				}
			}
		}
	}
}

// initClient 初始化客户端
func (n *Network) initClient() {
	n.clients = make(map[int32]*Client, len(n.producer.Client.Brokers))
	for brokerID, b := range n.producer.Client.Brokers {
		n.clients[brokerID] = &Client{
			broker:          b,
			inFlightRequest: queue.New(),
		}
	}
}

// handler 网络连接器处理
func (n *Network) handler() {
	for {
		func() {
			select {
			case data := <-n.input:
				n.producer.GlobalLock.RLock()
				defer n.producer.GlobalLock.RUnlock()
				n.toClient(data)
			case r := <-n.response:
				n.producer.GlobalLock.RLock()
				defer n.producer.GlobalLock.RUnlock()
				n.receive(r)
			}
		}()

	}
}

// toClient 发送消息
func (n *Network) toClient(data map[int32]*NetworkInput) {
	for brokerID, input := range data {
		client, ok := n.clients[brokerID]
		if !ok {
			continue
		}
		client.inFlightRequest.Add(input)
		if !client.isWait {
			client.isWait = true
			n.sendMessage(client)
		}
	}
}

// receive 接收
func (n *Network) receive(response *Response) {
	client, ok := n.clients[response.brokerID]
	// 不存在就忽略
	if !ok {
		return
	}
	// 重试也忽略
	if client.isRetry {
		if client.inFlightRequest.Length() > 0 {
			n.sendMessage(client)
		} else {
			client.isWait = false
		}
		client.isRetry = false
		return
	}
	// 取第一个元素
	request := client.inFlightRequest.Remove().(*NetworkInput)
	for topicName, partitions := range request.Input {
		for num, messages := range partitions {
			res := response.result[topicName][num]
			for i, m := range messages {
				re := &meta.MessageResponse{
					Partition: num,
					Offset:    res.Offset + int64(i) + 1,
					Err:       res.Err,
				}
				// 同步
				if m.SyncSign != nil {
					m.SyncSign <- re
					continue
				}
				// 异步
				m.Callback(re)
			}
		}
	}

	// 看是否需要等待
	if client.inFlightRequest.Length() == 0 {
		client.isWait = false
		return
	}

	n.sendMessage(client)
}

// sendMessage 发送消息
func (n *Network) sendMessage(client *Client) {
	method.SendMessage(client.broker, client.inFlightRequest.Peek().(*NetworkInput).Request,
		func(result *network.Response) error {
			r := result.ProtocolBody.(*response.ProduceResponse)
			n.response <- &Response{
				brokerID: client.broker.ID,
				result:   r.Blocks,
			}
			return nil
		})
}
