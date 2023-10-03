package meta

import (
	"fmt"
	"kafkacom-exercises/network"
	"kafkacom-exercises/util/coder/decoder"
	"kafkacom-exercises/util/coder/encoder"
	"net"
	"strconv"
	"sync"
)

// Broker 实例信息
type Broker struct {
	Rack *string

	ID            int32 // 实例ID标识
	Addr          string
	CorrelationID int32 // 记录该实例当前进行的会话id
	ClientID      string
	Conn          net.Conn `json:"-"`

	Responses chan *network.Response `json:"-"`

	sync.Mutex
}

// Decode 实例信息解析，格式为 实例id|ip|port|rack
func (b *Broker) Decode(pd decoder.PacketDecoder) (err error) {
	b.ID, err = pd.GetInt32()
	if err != nil {
		return err
	}

	host, err := pd.GetString()
	if err != nil {
		return err
	}

	port, err := pd.GetInt32()
	if err != nil {
		return err
	}

	// 机架id，可能会有
	b.Rack, err = pd.GetNullableString()
	if err != nil {
		return err
	}

	b.Addr = net.JoinHostPort(host, fmt.Sprint(port))
	if _, _, err := net.SplitHostPort(b.Addr); err != nil {
		return err
	}
	return nil
}

// Encode 实例信息编码
func (b *Broker) Encode(pe encoder.PacketEncoder) (err error) {
	host, portstr, err := net.SplitHostPort(b.Addr)
	if err != nil {
		return err
	}

	port, err := strconv.ParseInt(portstr, 10, 32)
	if err != nil {
		return err
	}

	pe.PutInt32(b.ID)

	err = pe.PutString(host)
	if err != nil {
		return err
	}

	pe.PutInt32(int32(port))

	err = pe.PutNullableString(b.Rack)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) Open() {
	b.Conn, _ = net.Dial("tcp", b.Addr)
	b.Responses = make(chan *network.Response, 10)

	// 接收请求回来的响应
	go func() {
		for response := range b.Responses {
			b.CorrelationID++
			// 先解析返回值
			if err := response.Decode(b.Conn); err != nil {
				fmt.Printf("数据解析出错了: %+v\n", err)
				continue
			}

			// 如果同步，就通知同步等待的信道，由同步位置进行处理
			if response.SyncSign != nil {
				response.SyncSign <- struct{}{}
				continue
			}

			// 如果异步的换，判断是否有回调函数
			if response.Request.Callback != nil {
				if err := response.Request.Callback(response); err != nil {
					fmt.Printf("回调出错了: %+v\n", err)
				}
			}
		}
	}()
}

func (b *Broker) Close() {
	b.Conn.Close()
	close(b.Responses)
}
