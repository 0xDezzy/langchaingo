package neo4j

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/tmc/langchaingo/graphs"
)

// TransactionManager handles transaction operations with context cancellation
type TransactionManager struct {
	neo4j *Neo4j
}

// newTransactionManager creates a new transaction manager
func newTransactionManager(n *Neo4j) *TransactionManager {
	return &TransactionManager{neo4j: n}
}

// ExplicitTransaction represents an explicit transaction
type ExplicitTransaction struct {
	tx      neo4j.ExplicitTransaction
	session neo4j.SessionWithContext
	ctx     context.Context
	cancel  context.CancelFunc
}

// WithTransaction executes a function within a transaction context
func (tm *TransactionManager) WithTransaction(ctx context.Context, fn func(tx neo4j.ManagedTransaction) error) error {
	if tm.neo4j.driver == nil {
		return ErrDriverNotInitialized
	}

	// Create session
	session := tm.neo4j.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: tm.neo4j.database,
	})
	defer session.Close(ctx)

	// Execute within transaction
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, fn(tx)
	})
	return err
}

// WithTimeoutTransaction executes a function within a transaction with timeout
func (tm *TransactionManager) WithTimeoutTransaction(ctx context.Context, timeout time.Duration, fn func(tx neo4j.ManagedTransaction) error) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	return tm.WithTransaction(ctx, fn)
}

// BeginTransaction starts an explicit transaction that can be manually managed
func (tm *TransactionManager) BeginTransaction(ctx context.Context) (*ExplicitTransaction, error) {
	if tm.neo4j.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	// Create cancellable context
	txCtx, cancel := context.WithCancel(ctx)

	// Create session
	session := tm.neo4j.driver.NewSession(txCtx, neo4j.SessionConfig{
		DatabaseName: tm.neo4j.database,
	})

	// Begin transaction
	tx, err := session.BeginTransaction(txCtx)
	if err != nil {
		session.Close(txCtx)
		cancel()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &ExplicitTransaction{
		tx:      tx,
		session: session,
		ctx:     txCtx,
		cancel:  cancel,
	}, nil
}

// Commit commits the explicit transaction
func (et *ExplicitTransaction) Commit() error {
	defer et.cleanup()
	return et.tx.Commit(et.ctx)
}

// Rollback rolls back the explicit transaction
func (et *ExplicitTransaction) Rollback() error {
	defer et.cleanup()
	return et.tx.Rollback(et.ctx)
}

// Close cancels and cleans up the transaction
func (et *ExplicitTransaction) Close() error {
	defer et.cleanup()
	return et.tx.Close(et.ctx)
}

// Run executes a query within the explicit transaction
func (et *ExplicitTransaction) Run(query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
	return et.tx.Run(et.ctx, query, params)
}

// cleanup handles context cancellation and resource cleanup
func (et *ExplicitTransaction) cleanup() {
	if et.cancel != nil {
		et.cancel()
	}
	if et.session != nil {
		et.session.Close(context.Background())
	}
}

// AddGraphDocumentWithTransaction adds graph documents using transaction management
func (tm *TransactionManager) AddGraphDocumentWithTransaction(ctx context.Context, docs []graphs.GraphDocument, options ...graphs.Option) error {
	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	// Use explicit transaction for better control
	return tm.WithTransaction(ctx, func(tx neo4j.ManagedTransaction) error {
		return tm.processDocumentsInTransaction(ctx, tx, docs, opts)
	})
}

// processDocumentsInTransaction processes documents within a transaction
func (tm *TransactionManager) processDocumentsInTransaction(ctx context.Context, tx neo4j.ManagedTransaction, docs []graphs.GraphDocument, opts *graphs.Options) error {
	// Ensure base entity constraint if needed
	if tm.neo4j.baseEntityLabel {
		if err := tm.ensureBaseEntityConstraintTx(ctx, tx); err != nil {
			return fmt.Errorf("failed to ensure base entity constraint: %w", err)
		}
	}

	// Create batches for efficient processing
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	for i := 0; i < len(docs); i += batchSize {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		batch := docs[i:end]
		if err := tm.processBatchInTransaction(ctx, tx, batch, opts); err != nil {
			return err
		}
	}

	return nil
}

// processBatchInTransaction processes a batch of documents within a transaction
func (tm *TransactionManager) processBatchInTransaction(ctx context.Context, tx neo4j.ManagedTransaction, docs []graphs.GraphDocument, opts *graphs.Options) error {
	// Import nodes first
	for _, doc := range docs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := tm.importNodesInTransaction(ctx, tx, doc, opts); err != nil {
			return err
		}
	}

	// Then import relationships
	for _, doc := range docs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := tm.importRelationshipsInTransaction(ctx, tx, doc, opts); err != nil {
			return err
		}
	}

	return nil
}

// importNodesInTransaction imports nodes within a transaction
func (tm *TransactionManager) importNodesInTransaction(ctx context.Context, tx neo4j.ManagedTransaction, doc graphs.GraphDocument, opts *graphs.Options) error {
	if len(doc.Nodes) == 0 {
		return nil
	}

	// Generate query using the appropriate method
	query := tm.neo4j.getNodeImportQuery(opts.IncludeSource)

	// Prepare node data
	var nodeData []map[string]interface{}
	for _, node := range doc.Nodes {
		nodeData = append(nodeData, map[string]interface{}{
			"id":         node.ID,
			"type":       cleanString(node.Type),
			"properties": node.Properties,
		})
	}

	// Prepare parameters
	params := map[string]interface{}{
		"nodes": nodeData,
	}

	if opts.IncludeSource {
		params["document_id"] = generateDocumentID(doc.Source)
		params["document_text"] = doc.Source.PageContent
		params["document_metadata"] = doc.Source.Metadata
	}

	// Execute query within transaction
	_, err := tx.Run(ctx, query, params)
	if err != nil && isAPOCError(err) {
		return wrapAPOCError(err)
	}
	return err
}

// importRelationshipsInTransaction imports relationships within a transaction
func (tm *TransactionManager) importRelationshipsInTransaction(ctx context.Context, tx neo4j.ManagedTransaction, doc graphs.GraphDocument, opts *graphs.Options) error {
	if len(doc.Relationships) == 0 {
		return nil
	}

	// Generate query using the appropriate method
	query := tm.neo4j.getRelImportQuery()

	// Prepare relationship data
	var relData []map[string]interface{}
	for _, rel := range doc.Relationships {
		relData = append(relData, map[string]interface{}{
			"source":       rel.Source.ID,
			"source_label": cleanString(rel.Source.Type),
			"target":       rel.Target.ID,
			"target_label": cleanString(rel.Target.Type),
			"type":         cleanString(strings.ReplaceAll(strings.ToUpper(rel.Type), " ", "_")),
			"properties":   rel.Properties,
		})
	}

	params := map[string]interface{}{
		"relationships": relData,
	}

	// Execute query within transaction
	_, err := tx.Run(ctx, query, params)
	if err != nil && isAPOCError(err) {
		return wrapAPOCError(err)
	}
	return err
}

// ensureBaseEntityConstraintTx creates the base entity constraint within a transaction
func (tm *TransactionManager) ensureBaseEntityConstraintTx(ctx context.Context, tx neo4j.ManagedTransaction) error {
	if !tm.neo4j.baseEntityLabel {
		return nil
	}

	// Check if constraint already exists
	constraintQuery := "SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties WHERE $label IN labelsOrTypes AND $property IN properties"
	result, err := tx.Run(ctx, constraintQuery, map[string]interface{}{
		"label":    BASE_ENTITY_LABEL,
		"property": "id",
	})
	if err != nil {
		// Fallback: try to create constraint anyway
	} else {
		records, err := result.Collect(ctx)
		if err == nil && len(records) > 0 {
			// Constraint already exists
			return nil
		}
	}

	// Create constraint
	createConstraintQuery := fmt.Sprintf("CREATE CONSTRAINT IF NOT EXISTS FOR (b:`%s`) REQUIRE b.id IS UNIQUE", BASE_ENTITY_LABEL)
	_, err = tx.Run(ctx, createConstraintQuery, nil)
	return err
}

// PeriodicCommitQuery executes a query with periodic commits for large datasets
func (tm *TransactionManager) PeriodicCommitQuery(ctx context.Context, query string, params map[string]interface{}, batchSize int) error {
	if tm.neo4j.driver == nil {
		return ErrDriverNotInitialized
	}

	// Default batch size for periodic commits
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Create session
	session := tm.neo4j.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: tm.neo4j.database,
	})
	defer session.Close(ctx)

	// Use USING PERIODIC COMMIT for large data operations
	periodicQuery := fmt.Sprintf("USING PERIODIC COMMIT %d %s", batchSize, query)

	// Execute with timeout handling
	var result neo4j.ResultWithContext
	var err error

	if tm.neo4j.timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, tm.neo4j.timeout)
		defer cancel()
		result, err = session.Run(timeoutCtx, periodicQuery, params)
	} else {
		result, err = session.Run(ctx, periodicQuery, params)
	}

	if err != nil {
		return fmt.Errorf("%w: %v", ErrQueryExecution, err)
	}

	// Consume the result to ensure the query completes
	_, err = result.Consume(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrQueryExecution, err)
	}

	return nil
}
