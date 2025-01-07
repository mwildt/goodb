package memtable

type memtableConfiguration struct {
	datadir           string
	logSuffix         string
	compactThreshold  int
	enableAutoCompact bool
}

type ConfigOption func(*memtableConfiguration)

func newConfig(options []ConfigOption) memtableConfiguration {
	config := memtableConfiguration{
		datadir:           "./data",
		logSuffix:         "mtlog",
		compactThreshold:  100,
		enableAutoCompact: true,
	}
	for _, opt := range options {
		opt(&config)
	}
	return config
}

func WithDatadir(value string) ConfigOption {

	return func(c *memtableConfiguration) {
		c.datadir = value
	}
}

func WithCompactThreshold(value int) ConfigOption {
	return func(c *memtableConfiguration) {
		c.compactThreshold = value
	}
}

func WithDisableAutoCompaction() ConfigOption {
	return func(c *memtableConfiguration) {
		c.enableAutoCompact = false
	}
}
