package clover

// Config contains clover configuration parameters
type Config struct {
	InMemory bool
	Storage  StorageEngine
}

func defaultConfig() *Config {
	return &Config{
		InMemory: false,
		Storage:  newStorageImpl(),
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
		if enable {
			c.Storage = newInMemoryStoreEngine()
		} else {
			c.Storage = newStorageImpl()
		}
		c.InMemory = enable
		return nil
	}
}

// WithStorageEngine allows to specify a custom storage engine.
func WithStorageEngine(engine StorageEngine) Option {
	return func(c *Config) error {
		if engine != nil {
			c.Storage = engine
		}
		return nil
	}
}
