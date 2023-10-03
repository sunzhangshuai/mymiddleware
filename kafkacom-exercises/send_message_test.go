package kafkacom_exercises

import (
	"kafkacom-exercises/meta"
	"kafkacom-exercises/producer"
	"strconv"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	p, _ := producer.NewProducer("127.0.0.1:9092")

	// 同步消息发送
	for i := 0; i < 10; i++ {
		t.Log("第", i+1, "条同步消息：开始发送")
		t.Log(p.SendMessage("test", "老婆你好，第"+strconv.Itoa(i+1)+"句", nil))
		t.Log("第", i+1, "条同步消息：结束发送")
	}

	// 异步发送消息
	for i := 10; i < 20; i++ {
		t.Log("第", i+1, "条异步消息：开始发送")
		index := i
		p.SendAsyncMessage("test", "老婆你好，第"+strconv.Itoa(i+1)+"句", nil, func(response *meta.MessageResponse) {
			t.Log(response.Partition, response.Offset, response.Err)
			t.Log("第", index+1, "条异步消息：结束发送")
		})
	}
	time.Sleep(3 * time.Second)

	for i := 20; i < 50; i++ {
		time.Sleep(300 * time.Millisecond)
		t.Log("第", i+1, "条异步消息：开始发送")
		index := i
		p.SendAsyncMessage("test", "老婆你好，第"+strconv.Itoa(i+1)+"句", nil, func(response *meta.MessageResponse) {
			t.Log(response.Partition, response.Offset, response.Err)
			t.Log("第", index+1, "条异步消息：结束发送")
		})
	}
	time.Sleep(10 * time.Second)
}
