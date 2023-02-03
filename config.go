package clover

import (
	"github.com/ostafen/clover/v2/store"
)

// Config contains clover configuration parameters
type Config struct {
	store store.Store
}

func defaultConfig() *Config {
	return &Config{
		store: nil,
	}
}

func (c *Config) applyOptions(opts []Option) (*Config, error) {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Option is a function that takes a config struct and modifies it
type Option func(c *Config) error

func WithStore(store store.Store) Option {
	return func(c *Config) error {
		c.store = store
		return nil
	}
}
