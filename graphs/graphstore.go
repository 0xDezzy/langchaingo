package graphs

import "context"

// RelationshipIdentifier uniquely identifies a relationship in the graph.
type RelationshipIdentifier struct {
	SourceID string
	TargetID string
	Type     string
}

// GraphStore defines the interface for graph database operations.
type GraphStore interface {
	// AddGraphDocument adds graph documents to the store.
	AddGraphDocument(ctx context.Context, docs []GraphDocument, options ...Option) error

	// AddNodes adds individual nodes to the graph store.
	AddNodes(ctx context.Context, nodes []Node, options ...Option) error

	// AddRelationships adds individual relationships to the graph store.
	AddRelationships(ctx context.Context, relationships []Relationship, options ...Option) error

	// UpdateNode updates an existing node in the graph store.
	UpdateNode(ctx context.Context, nodeID string, properties map[string]interface{}, options ...Option) error

	// UpdateRelationship updates an existing relationship in the graph store.
	UpdateRelationship(ctx context.Context, sourceID, targetID, relType string, properties map[string]interface{}, options ...Option) error

	// RemoveNode removes a node and all its relationships from the graph store.
	RemoveNode(ctx context.Context, nodeID string, options ...Option) error

	// RemoveNodes removes multiple nodes and their relationships from the graph store.
	RemoveNodes(ctx context.Context, nodeIDs []string, options ...Option) error

	// RemoveRelationship removes a specific relationship from the graph store.
	RemoveRelationship(ctx context.Context, sourceID, targetID, relType string, options ...Option) error

	// RemoveRelationships removes multiple relationships from the graph store.
	RemoveRelationships(ctx context.Context, relationships []RelationshipIdentifier, options ...Option) error

	// GetNode retrieves a node by its ID.
	GetNode(ctx context.Context, nodeID string, options ...Option) (*Node, error)

	// GetNodes retrieves multiple nodes by their IDs.
	GetNodes(ctx context.Context, nodeIDs []string, options ...Option) ([]Node, error)

	// GetRelationships retrieves relationships between nodes.
	GetRelationships(ctx context.Context, sourceID, targetID string, relType string, options ...Option) ([]Relationship, error)

	// GetNodesByType retrieves all nodes of a specific type.
	GetNodesByType(ctx context.Context, nodeType string, options ...Option) ([]Node, error)

	// GetRelationshipsByType retrieves all relationships of a specific type.
	GetRelationshipsByType(ctx context.Context, relType string, options ...Option) ([]Relationship, error)

	// NodeExists checks if a node exists in the graph store.
	NodeExists(ctx context.Context, nodeID string, options ...Option) (bool, error)

	// RelationshipExists checks if a relationship exists in the graph store.
	RelationshipExists(ctx context.Context, sourceID, targetID, relType string, options ...Option) (bool, error)

	// Query executes a query against the graph store. and returns the results.
	Query(ctx context.Context, query string, params map[string]interface{}) (map[string]interface{}, error)

	// RefreshSchema refreshes the schema information from the graph database.
	RefreshSchema(ctx context.Context) error

	// GetSchema returns the current schema as a string representation.
	GetSchema() string

	// GetStructuredSchema returns the structured schema information.
	GetStructuredSchema() map[string]interface{}

	// Close closes the graph store connection.
	Close() error
}

// Option defines functional options for graph store operations.
type Option func(*Options)

// Options contains configuration options for graph store operations.
type Options struct {
	// IncludeSource indicates whether to include source document information
	IncludeSource bool
	// BatchSize specifies the batch size for bulk operations
	BatchSize int
	// Timeout specifies the timeout in milliseconds
	Timeout int
	// MergeMode indicates how to handle existing nodes/relationships
	MergeMode MergeMode
	// CascadeDelete indicates whether to cascade delete related entities
	CascadeDelete bool
	// IncludeProperties specifies which properties to include in results
	IncludeProperties []string
	// ExcludeProperties specifies which properties to exclude from results
	ExcludeProperties []string
	// Limit specifies the maximum number of results to return
	Limit int
	// Offset specifies the number of results to skip
	Offset int
}

// MergeMode defines how to handle existing entities during operations.
type MergeMode int

const (
	// MergeModeCreate creates new entities, fails if they already exist
	MergeModeCreate MergeMode = iota
	// MergeModeUpdate updates existing entities, fails if they don't exist
	MergeModeUpdate
	// MergeModeUpsert creates new or updates existing entities
	MergeModeUpsert
	// MergeModeReplace replaces existing entities completely
	MergeModeReplace
)

// NewOptions create a new Options instance with default values.
func NewOptions() *Options {
	return &Options{
		IncludeSource:     false,
		BatchSize:         100,
		Timeout:           0, // No timeout by default
		MergeMode:         MergeModeUpsert,
		CascadeDelete:     false,
		IncludeProperties: nil, // Include all properties by default
		ExcludeProperties: nil,
		Limit:             0, // No limit by default
		Offset:            0,
	}
}

// WithIncludeSource sets whether to include source document information.
func WithIncludeSource(include bool) Option {
	return func(opts *Options) {
		opts.IncludeSource = include
	}
}

// WithBatchSize sets the batch size for bulk operations.
func WithBatchSize(size int) Option {
	return func(opts *Options) {
		opts.BatchSize = size
	}
}

// WithTimeout sets the query timeout in milliseconds.
func WithTimeout(timeout int) Option {
	return func(opts *Options) {
		opts.Timeout = timeout
	}
}

// WithMergeMode sets how to handle existing entities during operations.
func WithMergeMode(mode MergeMode) Option {
	return func(opts *Options) {
		opts.MergeMode = mode
	}
}

// WithCascadeDelete sets whether to cascade delete related entities.
func WithCascadeDelete(cascade bool) Option {
	return func(opts *Options) {
		opts.CascadeDelete = cascade
	}
}

// WithIncludeProperties sets which properties to include in results.
func WithIncludeProperties(properties []string) Option {
	return func(opts *Options) {
		opts.IncludeProperties = properties
	}
}

// WithExcludeProperties sets which properties to exclude from results.
func WithExcludeProperties(properties []string) Option {
	return func(opts *Options) {
		opts.ExcludeProperties = properties
	}
}

// WithLimit sets the maximum number of results to return.
func WithLimit(limit int) Option {
	return func(opts *Options) {
		opts.Limit = limit
	}
}

// WithOffset sets the number of results to skip.
func WithOffset(offset int) Option {
	return func(opts *Options) {
		opts.Offset = offset
	}
}
