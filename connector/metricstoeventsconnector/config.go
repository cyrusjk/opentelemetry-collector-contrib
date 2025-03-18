package metricstoeventsconnector

import (
	"fmt"
)

type Config struct {
	UseCaching bool `mapstructure:"use_caching"`
}

func (c *Config) Validate() error {
	if c.UseCaching {
		fmt.Println("UseCaching is enabled")
	}
	return nil
}
