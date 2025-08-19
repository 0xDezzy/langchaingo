package neo4j

import (
	"fmt"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	// LIST_LIMIT is the maximum size of lists to include in results
	LIST_LIMIT = 128
	// EXHAUSTIVE_SEARCH_LIMIT determines when to do exhaustive vs sampling search
	EXHAUSTIVE_SEARCH_LIMIT = 10000
	// DISTINCT_VALUE_LIMIT is the threshold for returning all vs sample values
	DISTINCT_VALUE_LIMIT = 10
	// BASE_ENTITY_LABEL is the secondary label applied to all nodes for performance
	BASE_ENTITY_LABEL = "__Entity__"
)

var (
	ErrDriverNotInitialized = fmt.Errorf("neo4j driver not initialized")
	ErrInvalidURI           = fmt.Errorf("invalid neo4j URI")
	ErrConnectionFailed     = fmt.Errorf("failed to connect to neo4j")
	ErrQueryExecution       = fmt.Errorf("failed to execute query")
	ErrAPOCNotAvailable     = fmt.Errorf("APOC procedures not available")
)

// Neo4j implements the graphs.GraphStore interface for Neo4j
type Neo4j struct {
	// Neo4j driver for managing connections
	driver neo4j.DriverWithContext

	// Configuration options
	uri             string
	username        string
	password        string
	database        string
	sanitize        bool
	enhancedSchema  bool
	baseEntityLabel bool
	timeout         time.Duration

	// Schema cache
	schemaMux        sync.RWMutex
	schemaCache      string
	structuredSchema map[string]interface{}

	// Transaction manager
	txManager *TransactionManager

	// Configuration options
	config neo4j.Config
}

// newNeo4j creates a new Neo4j instance with the given configuration
func newNeo4j(opts ...Option) (*Neo4j, error) {
	options := &options{}

	// Apply options
	for _, opt := range opts {
		opt(options)
	}

	// Apply defaults for any unset values
	applyDefaults(options)

	// Create Neo4j instance
	n4j := &Neo4j{
		uri:              options.uri,
		username:         options.username,
		password:         options.password,
		database:         options.database,
		sanitize:         options.sanitize,
		enhancedSchema:   options.enhancedSchema,
		baseEntityLabel:  options.baseEntityLabel,
		timeout:          options.timeout,
		config:           options.config,
		structuredSchema: make(map[string]interface{}),
	}

	// Initialize driver
	if err := n4j.connect(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	// Initialize transaction manager
	n4j.txManager = newTransactionManager(n4j)

	return n4j, nil
}

// NewNeo4j creates a new Neo4j graph store
func NewNeo4j(opts ...Option) (*Neo4j, error) {
	return newNeo4j(opts...)
}

// TransactionManager returns the transaction manager for advanced transaction control
func (n *Neo4j) TransactionManager() *TransactionManager {
	return n.txManager
}
