package kafkacom_exercises

import (
	"fmt"
	"kafkacom-exercises/consumer"
	"kafkacom-exercises/meta"
	"kafkacom-exercises/producer"
	"strconv"
	"sync"
	"testing"
	"time"
)

//var topics = []string{"test_3"}

var topics = []string{"topic_23", "topic_33"}
var addr = "127.0.0.1:9093"
var group = "test_group"

func TestProducer(t *testing.T) {
	p, _ := producer.NewProducer(addr)
	for i := 0; ; i++ {
		time.Sleep(10 * time.Millisecond)
		for _, topic := range topics {
			message := "老婆你好，第" + strconv.Itoa(i+1) + "句"
			go func(topic string) {
				p.SendAsyncMessage(topic, message, nil, func(response *meta.MessageResponse) {
					t.Log(fmt.Sprintf("[%s][%d][%d]", topic, response.Partition, response.Err), message)
				})
			}(topic)
		}
	}
}

func TestConsumer(t *testing.T) {
	g, _ := consumer.NewConsumerGroup(addr, group)
	wg := sync.WaitGroup{}
	wg.Add(1)
	for i := 0; i < 25; i++ {
		go func(i int) {
			m := g.NewMember(fmt.Sprintf("consumer:%d", i), topics)
			m.Consumer(func(message *meta.Message) {
				m.Log("消费成功：[%s][%d][%d][%s]", message.Topic, message.PartitionNum, message.Offset, message.Value)
			})
		}(i)
		time.Sleep(10 * time.Second)
	}
	wg.Wait()

}
