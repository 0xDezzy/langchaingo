package kuzu

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kuzudb/go-kuzu"
	"github.com/tmc/langchaingo/graphs"
)

var (
	ErrDatabaseNotInitialized    = fmt.Errorf("kuzu database not initialized")
	ErrConnectionNotInitialized  = fmt.Errorf("kuzu connection not initialized")
	ErrDangerousRequestsDisabled = fmt.Errorf("dangerous requests are disabled - enable with WithAllowDangerousRequests(true)")
	ErrDatabaseCreationFailed    = fmt.Errorf("failed to create kuzu database")
	ErrConnectionCreationFailed  = fmt.Errorf("failed to create kuzu connection")
	ErrQueryExecutionFailed      = fmt.Errorf("failed to execute query")
	ErrSchemaRefreshFailed       = fmt.Errorf("failed to refresh schema")
)

// Kuzu implements the graphs.GraphStore interface for KuzuDB
type Kuzu struct {
	// KuzuDB database instance
	database *kuzu.Database

	// KuzuDB connection for queries
	connection *kuzu.Connection

	// Configuration options
	options *options

	// Schema cache
	schemaMux        sync.RWMutex
	schemaCache      string
	structuredSchema map[string]interface{}

	// Track created tables to avoid recreating
	tablesMux     sync.RWMutex
	nodeTables    map[string]bool
	relTables     map[string]bool
	relationTypes map[string]bool // Track relationship type combinations
}

// NewKuzu creates a new KuzuDB graph store instance
func NewKuzu(opts ...Option) (*Kuzu, error) {
	options := &options{}

	// Apply options
	for _, opt := range opts {
		opt(options)
	}

	// Apply defaults for any unset values
	applyDefaults(options)

	// Check for dangerous requests requirement
	if !options.allowDangerousRequests {
		return nil, ErrDangerousRequestsDisabled
	}

	// Create Kuzu instance
	k := &Kuzu{
		options:          options,
		structuredSchema: make(map[string]interface{}),
		nodeTables:       make(map[string]bool),
		relTables:        make(map[string]bool),
		relationTypes:    make(map[string]bool),
	}

	// Initialize database and connection
	if err := k.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to kuzu: %w", err)
	}

	return k, nil
}

// connect initializes the KuzuDB database and connection
func (k *Kuzu) connect() error {
	var err error

	// Create system config
	systemConfig := kuzu.DefaultSystemConfig()
	if k.options.bufferPoolSize > 0 {
		systemConfig.BufferPoolSize = k.options.bufferPoolSize
	}
	if k.options.maxNumThreads > 0 {
		systemConfig.MaxNumThreads = k.options.maxNumThreads
	}

	// Create database
	if k.options.inMemory {
		// Create in-memory database
		k.database, err = kuzu.OpenInMemoryDatabase(systemConfig)
	} else {
		// Create file-based database
		k.database, err = kuzu.OpenDatabase(k.options.databasePath, systemConfig)
	}

	if err != nil {
		return fmt.Errorf("%w: %v", ErrDatabaseCreationFailed, err)
	}

	// Create connection
	k.connection, err = kuzu.OpenConnection(k.database)
	if err != nil {
		k.database.Close()
		return fmt.Errorf("%w: %v", ErrConnectionCreationFailed, err)
	}

	// Configure connection settings
	if k.options.maxNumThreads > 0 {
		k.connection.SetMaxNumThreads(k.options.maxNumThreads)
	}

	if k.options.timeout > 0 {
		// Convert time.Duration to milliseconds (uint64)
		timeoutMs := uint64(k.options.timeout.Milliseconds())
		k.connection.SetTimeout(timeoutMs)
	}

	return nil
}

// Close closes the KuzuDB connection and database
func (k *Kuzu) Close() error {
	if k.connection != nil {
		k.connection.Close()
		k.connection = nil
	}

	if k.database != nil {
		k.database.Close()
		k.database = nil
	}

	return nil
}

// QueryWithRetry executes a query with automatic retry on connection failures
func (k *Kuzu) QueryWithRetry(ctx context.Context, query string, params map[string]interface{}, maxRetries int) (map[string]interface{}, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := k.Query(ctx, query, params)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if it's a connection error that might benefit from retry
		if err == ErrConnectionNotInitialized || err == ErrDatabaseNotInitialized {
			// Attempt to reconnect
			if reconnectErr := k.Reconnect(); reconnectErr != nil {
				lastErr = fmt.Errorf("reconnection failed: %w (original error: %v)", reconnectErr, err)
				continue
			}
		}

		// If not the last attempt, wait before retrying
		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
				// Exponential backoff starting at 100ms
			}
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", maxRetries+1, lastErr)
}

// IsConnected checks if the KuzuDB connection is active
func (k *Kuzu) IsConnected() bool {
	return k.connection != nil && k.database != nil
}

// Reconnect re-establishes the database connection
func (k *Kuzu) Reconnect() error {
	// Close existing connections
	if err := k.Close(); err != nil {
		return err
	}

	// Re-establish connection
	return k.connect()
}

// HealthCheck performs a basic health check on the database connection
func (k *Kuzu) HealthCheck(ctx context.Context) error {
	if !k.IsConnected() {
		return ErrConnectionNotInitialized
	}

	// Execute a simple query to verify connection health
	_, err := k.Query(ctx, "RETURN 1 as health_check", nil)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// Query executes a Cypher query against the KuzuDB database
// Parameters are supported through prepared statements for better performance and security
func (k *Kuzu) Query(ctx context.Context, query string, params map[string]interface{}) (map[string]interface{}, error) {
	if !k.IsConnected() {
		return nil, ErrConnectionNotInitialized
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Execute query with timeout handling
	done := make(chan struct{})
	var result *kuzu.QueryResult
	var err error

	go func() {
		defer close(done)

		if len(params) > 0 {
			// Use prepared statements for parameterized queries
			result, err = k.executeWithParameters(query, params)
		} else {
			// Execute simple query without parameters
			result, err = k.connection.Query(query)
		}
	}()

	// Wait for query completion or context cancellation
	select {
	case <-ctx.Done():
		// Context cancelled, interrupt query if possible
		k.connection.Interrupt()
		return nil, ctx.Err()
	case <-done:
		// Query completed
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrQueryExecutionFailed, err)
	}
	defer result.Close()

	// Convert result to expected format using helper function
	records, err := k.convertQueryResult(ctx, result)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"records": records,
		"summary": map[string]interface{}{
			"query":          query,
			"parameters":     params,
			"execution_time": result.GetExecutionTime(),
			"compiling_time": result.GetCompilingTime(),
			"number_of_rows": result.GetNumberOfRows(),
		},
	}, nil
}

// executeWithParameters executes a query using prepared statements with parameters
func (k *Kuzu) executeWithParameters(query string, params map[string]interface{}) (*kuzu.QueryResult, error) {
	// Prepare the statement
	stmt, err := k.connection.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Convert parameters to the expected format
	kuzuParams := make(map[string]any)
	for key, value := range params {
		kuzuParams[key] = value
	}

	// Execute with parameters
	result, err := k.connection.Execute(stmt, kuzuParams)
	if err != nil {
		return nil, fmt.Errorf("failed to execute prepared statement: %w", err)
	}

	return result, nil
}

// convertQueryResult converts KuzuDB query result to expected format
func (k *Kuzu) convertQueryResult(ctx context.Context, result *kuzu.QueryResult) ([]map[string]interface{}, error) {
	records := make([]map[string]interface{}, 0)

	for result.HasNext() {
		// Check for context cancellation during result processing
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		tuple, err := result.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next tuple: %w", err)
		}

		// Use GetAsMap for easy conversion to expected format
		record, err := tuple.GetAsMap()
		tuple.Close() // Close tuple immediately after processing
		if err != nil {
			return nil, fmt.Errorf("failed to convert tuple to map: %w", err)
		}

		records = append(records, record)
	}

	return records, nil
}

// PrepareQuery creates a prepared statement for the given query
// Useful for queries that will be executed multiple times with different parameters
func (k *Kuzu) PrepareQuery(query string) (*kuzu.PreparedStatement, error) {
	if !k.IsConnected() {
		return nil, ErrConnectionNotInitialized
	}

	stmt, err := k.connection.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	return stmt, nil
}

// ExecutePreparedQuery executes a prepared statement with the given parameters
func (k *Kuzu) ExecutePreparedQuery(ctx context.Context, stmt *kuzu.PreparedStatement, params map[string]interface{}) (map[string]interface{}, error) {
	if !k.IsConnected() {
		return nil, ErrConnectionNotInitialized
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Convert parameters to expected format
	kuzuParams := make(map[string]any)
	for key, value := range params {
		kuzuParams[key] = value
	}

	// Execute with context handling
	done := make(chan struct{})
	var result *kuzu.QueryResult
	var err error

	go func() {
		defer close(done)
		result, err = k.connection.Execute(stmt, kuzuParams)
	}()

	// Wait for execution completion or context cancellation
	select {
	case <-ctx.Done():
		k.connection.Interrupt()
		return nil, ctx.Err()
	case <-done:
		// Execution completed
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute prepared statement: %w", err)
	}
	defer result.Close()

	// Convert result to expected format
	records, err := k.convertQueryResult(ctx, result)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"records": records,
		"summary": map[string]interface{}{
			"parameters":     params,
			"execution_time": result.GetExecutionTime(),
			"compiling_time": result.GetCompilingTime(),
			"number_of_rows": result.GetNumberOfRows(),
		},
	}, nil
}

// QuerySingle executes a query expected to return a single result
// Returns an error if no results or multiple results are found
func (k *Kuzu) QuerySingle(ctx context.Context, query string, params map[string]interface{}) (map[string]interface{}, error) {
	result, err := k.Query(ctx, query, params)
	if err != nil {
		return nil, err
	}

	records, ok := result["records"].([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected result format")
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("query returned no results")
	}

	if len(records) > 1 {
		return nil, fmt.Errorf("query returned multiple results, expected single result")
	}

	return records[0], nil
}

// QueryCount executes a query and returns only the count of records
func (k *Kuzu) QueryCount(ctx context.Context, query string, params map[string]interface{}) (uint64, error) {
	result, err := k.Query(ctx, query, params)
	if err != nil {
		return 0, err
	}

	summary, ok := result["summary"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected result format")
	}

	count, ok := summary["number_of_rows"].(uint64)
	if !ok {
		return 0, fmt.Errorf("could not extract row count from result")
	}

	return count, nil
}

// AddGraphDocument adds graph documents to the KuzuDB store
func (k *Kuzu) AddGraphDocument(ctx context.Context, docs []graphs.GraphDocument, options ...graphs.Option) error {
	if k.connection == nil {
		return ErrConnectionNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	// Process each document
	for _, doc := range docs {
		if err := k.processGraphDocument(ctx, doc, opts); err != nil {
			return err
		}
	}

	return nil
}

// processGraphDocument processes a single graph document with batch optimization
func (k *Kuzu) processGraphDocument(ctx context.Context, doc graphs.GraphDocument, opts *graphs.Options) error {
	// Validate document first
	if err := k.validateGraphDocument(doc); err != nil {
		return fmt.Errorf("document validation failed: %w", err)
	}

	// Create tables for nodes and relationships first
	if err := k.ensureTablesExist(doc); err != nil {
		return err
	}

	// Add source document if required
	// TODO: Fix Chunk table and MENTIONS functionality for KuzuDB
	if opts.IncludeSource {
		// Skip source document functionality for now
		// if err := k.addSourceDocument(doc); err != nil {
		//	return err
		// }
	}

	// Use batch processing for better performance
	if len(doc.Nodes) > 10 || len(doc.Relationships) > 10 {
		return k.processBatch(ctx, doc, opts)
	}

	// For small documents, process individually for simplicity
	return k.processIndividually(ctx, doc, opts)
}

// processBatch handles large documents with batch processing
func (k *Kuzu) processBatch(ctx context.Context, doc graphs.GraphDocument, opts *graphs.Options) error {
	// Group nodes by type for efficient batch insertion
	nodesByType := make(map[string][]graphs.Node)
	for _, node := range doc.Nodes {
		nodesByType[node.Type] = append(nodesByType[node.Type], node)
	}

	// Batch insert nodes by type
	for nodeType, nodes := range nodesByType {
		if err := k.batchInsertNodesByType(ctx, nodeType, nodes); err != nil {
			return fmt.Errorf("failed to batch insert nodes of type %s: %w", nodeType, err)
		}
	}

	// Create source links if required
	if opts.IncludeSource {
		if err := k.batchLinkNodesToSource(ctx, doc.Nodes, doc); err != nil {
			return fmt.Errorf("failed to batch link nodes to source: %w", err)
		}
	}

	// Group relationships by type for batch insertion
	relsByType := make(map[string][]graphs.Relationship)
	for _, rel := range doc.Relationships {
		key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
		relsByType[key] = append(relsByType[key], rel)
	}

	// Batch insert relationships by type
	for _, rels := range relsByType {
		if err := k.batchInsertRelationships(ctx, rels); err != nil {
			return fmt.Errorf("failed to batch insert relationships: %w", err)
		}
	}

	return nil
}

// processIndividually handles small documents with individual processing
func (k *Kuzu) processIndividually(ctx context.Context, doc graphs.GraphDocument, opts *graphs.Options) error {
	// Add nodes
	for _, node := range doc.Nodes {
		if err := k.addNode(node); err != nil {
			return err
		}

		// Link to source document if required
		// TODO: Fix MENTIONS functionality for KuzuDB
		if opts.IncludeSource {
			// Skip MENTIONS functionality for now
			// if err := k.linkNodeToSource(node, doc); err != nil {
			//	return err
			// }
		}
	}

	// Add relationships
	for _, rel := range doc.Relationships {
		if err := k.addRelationship(rel); err != nil {
			return err
		}
	}

	return nil
}

// RefreshSchema refreshes the schema information from the KuzuDB database
func (k *Kuzu) RefreshSchema(ctx context.Context) error {
	if k.connection == nil {
		return ErrConnectionNotInitialized
	}

	k.schemaMux.Lock()
	defer k.schemaMux.Unlock()

	// Introspect the current schema
	if err := k.introspectSchema(); err != nil {
		return fmt.Errorf("failed to introspect schema: %w", err)
	}

	// Build human-readable schema description
	k.buildSchemaDescription()

	return nil
}

// GetSchema returns the current schema as a string representation
func (k *Kuzu) GetSchema() string {
	k.schemaMux.RLock()
	defer k.schemaMux.RUnlock()
	return k.schemaCache
}

// GetStructuredSchema returns the structured schema information
func (k *Kuzu) GetStructuredSchema() map[string]interface{} {
	k.schemaMux.RLock()
	defer k.schemaMux.RUnlock()

	// Return a copy to prevent external modification
	result := make(map[string]interface{})
	for k, v := range k.structuredSchema {
		result[k] = v
	}
	return result
}

// Helper methods are implemented in separate files:
// - schema.go: Schema management and table creation
// - import.go: Graph document import and node/relationship management
// - types.go: Type conversion and data type handling
// - transaction.go: Transaction management and ACID operations

// QueryWithTypes executes a query with automatic type conversion for results
func (k *Kuzu) QueryWithTypes(ctx context.Context, query string, params map[string]interface{}) (map[string]interface{}, error) {
	// Execute the query normally
	result, err := k.Query(ctx, query, params)
	if err != nil {
		return nil, err
	}

	// Apply type conversion to results
	typeConverter := NewTypeConverter()

	if records, ok := result["records"].([]map[string]interface{}); ok {
		convertedRecords := make([]map[string]interface{}, len(records))

		for i, record := range records {
			convertedRecord := make(map[string]interface{})
			for key, value := range record {
				// Try to convert back to appropriate Go types
				if convertedValue, err := typeConverter.ConvertKuzuValueToGo(value, KuzuSTRING); err == nil {
					convertedRecord[key] = convertedValue
				} else {
					convertedRecord[key] = value // Fallback to original
				}
			}
			convertedRecords[i] = convertedRecord
		}

		result["records"] = convertedRecords
	}

	return result, nil
}

// GetTypeConverter returns a configured type converter instance
func (k *Kuzu) GetTypeConverter() *TypeConverter {
	return NewTypeConverter()
}

// GetPropertyConverter returns a configured property converter instance
func (k *Kuzu) GetPropertyConverter() *PropertyConverter {
	return NewPropertyConverter()
}

// ValidatePropertyTypes validates that properties are compatible with KuzuDB types
func (k *Kuzu) ValidatePropertyTypes(properties map[string]interface{}) error {
	propertyConverter := NewPropertyConverter()
	_, err := propertyConverter.ConvertProperties(properties)
	return err
}
