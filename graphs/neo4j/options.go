package neo4j

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	// Environment variable names
	Neo4jURIEnvVarName      = "NEO4J_URI"
	Neo4jUsernameEnvVarName = "NEO4J_USERNAME"
	Neo4jPasswordEnvVarName = "NEO4J_PASSWORD"
	Neo4jDatabaseEnvVarName = "NEO4J_DATABASE"

	// Default values
	DefaultURI      = "bolt://localhost:7687"
	DefaultUsername = "neo4j"
	DefaultPassword = "password"
	DefaultDatabase = "neo4j"
)

var (
	ErrInvalidOptions = errors.New("invalid neo4j options")
)

// Option is a function type that can be used to modify the Neo4j configuration.
type Option func(*options)

// options holds the configuration for Neo4j connections.
type options struct {
	uri             string
	username        string
	password        string
	database        string
	sanitize        bool
	enhancedSchema  bool
	baseEntityLabel bool
	timeout         time.Duration
	config          neo4j.Config
}

// WithURI sets the Neo4j connection URI.
func WithURI(uri string) Option {
	return func(o *options) {
		o.uri = uri
	}
}

// WithHost sets the Neo4j host (combines with port to create URI).
func WithHost(host string) Option {
	return func(o *options) {
		o.uri = fmt.Sprintf("bolt://%s:7687", host)
	}
}

// WithHostAndPort sets the Neo4j host and port (creates bolt:// URI).
func WithHostAndPort(host string, port int) Option {
	return func(o *options) {
		o.uri = fmt.Sprintf("bolt://%s:%d", host, port)
	}
}

// WithAuth sets the authentication credentials.
func WithAuth(username, password string) Option {
	return func(o *options) {
		o.username = username
		o.password = password
	}
}

// WithUsername sets the username for authentication.
func WithUsername(username string) Option {
	return func(o *options) {
		o.username = username
	}
}

// WithPassword sets the password for authentication.
func WithPassword(password string) Option {
	return func(o *options) {
		o.password = password
	}
}

// WithDatabase sets the Neo4j database name.
func WithDatabase(database string) Option {
	return func(o *options) {
		o.database = database
	}
}

// WithMaxConnectionLifetime sets the maximum lifetime for connections.
func WithMaxConnectionLifetime(lifetime time.Duration) Option {
	return func(o *options) {
		o.config.MaxConnectionLifetime = lifetime
	}
}

// WithMaxConnectionPoolSize sets the maximum connection pool size.
func WithMaxConnectionPoolSize(size int) Option {
	return func(o *options) {
		o.config.MaxConnectionPoolSize = size
	}
}

// WithConnectionAcquisitionTimeout sets the timeout for acquiring connections.
func WithConnectionAcquisitionTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.config.ConnectionAcquisitionTimeout = timeout
	}
}

// WithConfig allows setting a custom Neo4j driver configuration.
func WithConfig(config neo4j.Config) Option {
	return func(o *options) {
		o.config = config
	}
}

// WithSanitize enables or disables value sanitization for query results.
// When enabled, removes oversized lists and embedding-like values to improve LLM performance.
func WithSanitize(sanitize bool) Option {
	return func(o *options) {
		o.sanitize = sanitize
	}
}

// WithEnhancedSchema enables enhanced schema generation with property value sampling.
// When enabled, includes example values, min/max ranges, and distinct counts in schema.
func WithEnhancedSchema(enhanced bool) Option {
	return func(o *options) {
		o.enhancedSchema = enhanced
	}
}

// WithTimeout sets the timeout for Neo4j queries.
// Useful for terminating long-running queries. Zero value means no timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.timeout = timeout
	}
}

// WithBaseEntityLabel enables base entity labeling for improved performance.
// When enabled, all nodes get a secondary __Entity__ label with unique constraints.
func WithBaseEntityLabel(enable bool) Option {
	return func(o *options) {
		o.baseEntityLabel = enable
	}
}

// New creates a new Neo4j GraphStore instance with the given options.
func New(opts ...Option) (*Neo4j, error) {
	return newNeo4j(opts...)
}

// getFromDictOrEnv gets a value from options, environment variable, or default value.
// This mimics the Python implementation's get_from_dict_or_env utility.
func getFromDictOrEnv(optValue, envVarName, defaultValue string) string {
	if optValue != "" {
		return optValue
	}
	if envValue := os.Getenv(envVarName); envValue != "" {
		return envValue
	}
	return defaultValue
}

// applyDefaults applies default configuration values from environment variables or constants.
func applyDefaults(o *options) {
	o.uri = getFromDictOrEnv(o.uri, Neo4jURIEnvVarName, DefaultURI)
	o.username = getFromDictOrEnv(o.username, Neo4jUsernameEnvVarName, DefaultUsername)
	o.password = getFromDictOrEnv(o.password, Neo4jPasswordEnvVarName, DefaultPassword)
	o.database = getFromDictOrEnv(o.database, Neo4jDatabaseEnvVarName, DefaultDatabase)
}
