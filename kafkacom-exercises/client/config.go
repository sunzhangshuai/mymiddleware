package client

import "kafkacom-exercises/util"

// Config https://t77893.com:29875/category/?category_id=41
type Config struct {
	ClientID string
}

func NewConfig() *Config {
	return &Config{
		ClientID: util.ClientID,
	}
}
