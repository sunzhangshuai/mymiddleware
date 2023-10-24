package client

import (
	"kafkacom-exercises/consumer/balance"
	"kafkacom-exercises/util"
	"time"
)

// Config https://t77893.com:29875/category/?category_id=41
type Config struct {
	ClientID string
	Consumer struct {
		Session struct {
			Timeout time.Duration
		}
		Rebalance struct {
			Timeout  time.Duration
			Strategy balance.Strategy
			Retry    int
		}
		Heartbeat struct {
			Timeout time.Duration
		}
		Fetch struct {
			Min int32
		}
		Client struct {
			MaxLen int64
		}
		MaxWaitTime time.Duration
	}
}

func NewConfig() *Config {
	c := new(Config)
	c.ClientID = util.ClientID
	c.Consumer.Session.Timeout = 10 * time.Second
	c.Consumer.Rebalance.Timeout = 60 * time.Second
	c.Consumer.Rebalance.Strategy = balance.NewBalanceStrategyRange()
	c.Consumer.Rebalance.Retry = 5
	c.Consumer.Heartbeat.Timeout = 3 * time.Second
	c.Consumer.Fetch.Min = 1
	c.Consumer.MaxWaitTime = 500 * time.Millisecond
	c.Consumer.Client.MaxLen = 1000
	return c
}
