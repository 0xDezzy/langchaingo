package kuzu

import "time"

// Option defines functional options for KuzuDB configuration.
type Option func(*options)

// options contains configuration options for KuzuDB graph store.
type options struct {
	// Database path for file-based storage (empty for in-memory)
	databasePath string

	// Whether to use in-memory database
	inMemory bool

	// Whether to allow potentially dangerous requests
	allowDangerousRequests bool

	// Query timeout
	timeout time.Duration

	// Buffer pool size for KuzuDB
	bufferPoolSize uint64

	// Maximum number of threads for query execution
	maxNumThreads uint64

	// Whether to enable logging
	enableLogging bool

	// Log level for KuzuDB operations
	logLevel string
}

// applyDefaults sets default values for any unset options.
func applyDefaults(opts *options) {
	if opts.databasePath == "" && !opts.inMemory {
		opts.databasePath = "./kuzu_db"
	}

	if opts.timeout == 0 {
		opts.timeout = 30 * time.Second
	}

	if opts.bufferPoolSize == 0 {
		opts.bufferPoolSize = 1024 * 1024 * 1024 // 1GB default
	}

	if opts.maxNumThreads == 0 {
		opts.maxNumThreads = 4 // Default to 4 threads
	}

	if opts.logLevel == "" {
		opts.logLevel = "info"
	}
}

// WithDatabasePath sets the file path for the KuzuDB database.
// If not set, defaults to "./kuzu_db".
func WithDatabasePath(path string) Option {
	return func(opts *options) {
		opts.databasePath = path
		opts.inMemory = false
	}
}

// WithInMemory configures KuzuDB to run in-memory mode.
// This is useful for testing and temporary graph operations.
func WithInMemory(inMemory bool) Option {
	return func(opts *options) {
		opts.inMemory = inMemory
		if inMemory {
			opts.databasePath = ""
		}
	}
}

// WithAllowDangerousRequests enables potentially dangerous operations.
// This should be used carefully in production environments.
// Required to enable arbitrary Cypher query execution.
func WithAllowDangerousRequests(allow bool) Option {
	return func(opts *options) {
		opts.allowDangerousRequests = allow
	}
}

// WithTimeout sets the query execution timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.timeout = timeout
	}
}

// WithBufferPoolSize sets the buffer pool size for KuzuDB.
// This affects memory usage and performance.
func WithBufferPoolSize(size uint64) Option {
	return func(opts *options) {
		opts.bufferPoolSize = size
	}
}

// WithMaxNumThreads sets the maximum number of threads for query execution.
func WithMaxNumThreads(threads uint64) Option {
	return func(opts *options) {
		opts.maxNumThreads = threads
	}
}

// WithEnableLogging enables or disables logging for KuzuDB operations.
func WithEnableLogging(enable bool) Option {
	return func(opts *options) {
		opts.enableLogging = enable
	}
}

// WithLogLevel sets the log level for KuzuDB operations.
// Valid levels: "trace", "debug", "info", "warn", "error".
func WithLogLevel(level string) Option {
	return func(opts *options) {
		opts.logLevel = level
	}
}
