package memtable

type MigrationObject map[string]interface{}

type memtableConfiguration struct {
	datadir           string
	logSuffix         string
	compactThreshold  int
	enableAutoCompact bool
	migrations        []Migration[MigrationObject]
}

type ConfigOption func(*memtableConfiguration)

func newConfig(options []ConfigOption) memtableConfiguration {
	config := memtableConfiguration{
		datadir:           "./data",
		logSuffix:         "mtlog",
		compactThreshold:  100,
		enableAutoCompact: true,
		migrations:        make([]Migration[MigrationObject], 0),
	}
	for _, opt := range options {
		opt(&config)
	}
	return config
}

func WithMigration(name, version string, handler func(MigrationObject) (MigrationObject, error)) ConfigOption {
	return func(c *memtableConfiguration) {
		c.migrations = append(c.migrations, Migration[MigrationObject]{name, version, handler})
	}
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
