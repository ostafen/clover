package clover

import "time"

const (
	GCReclaimIntervalDefault = time.Minute * 5
	GCDiscardRatioDefault    = 0.5
)

// Config contains clover configuration parameters
type Config struct {
	InMemory          bool
	GCReclaimInterval time.Duration
	GCDiscardRatio    float64
}

func defaultConfig() *Config {
	return &Config{
		InMemory:          false,
		GCReclaimInterval: GCReclaimIntervalDefault,
		GCDiscardRatio:    GCDiscardRatioDefault,
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

// InMemoryMode allows to enable/disable in-memory mode.
func InMemoryMode(enable bool) Option {
	return func(c *Config) error {
		c.InMemory = enable
		return nil
	}
}
