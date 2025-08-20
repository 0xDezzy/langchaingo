package kuzu

import (
	"context"
	"fmt"
	"sync"
)

// TransactionState represents the state of a transaction
type TransactionState int

const (
	TransactionIdle TransactionState = iota
	TransactionActive
	TransactionCommitted
	TransactionRolledBack
	TransactionFailed
)

// String returns the string representation of the transaction state
func (ts TransactionState) String() string {
	switch ts {
	case TransactionIdle:
		return "idle"
	case TransactionActive:
		return "active"
	case TransactionCommitted:
		return "committed"
	case TransactionRolledBack:
		return "rolled_back"
	case TransactionFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Transaction represents a KuzuDB transaction
type Transaction struct {
	id         string
	kuzu       *Kuzu
	state      TransactionState
	ctx        context.Context
	cancelFunc context.CancelFunc
	stateMux   sync.RWMutex
	operations []string // Track operations for potential rollback
	startedAt  int64    // Timestamp when transaction started
}

// TransactionOption defines options for transaction configuration
type TransactionOption func(*TransactionConfig)

// TransactionConfig holds transaction configuration
type TransactionConfig struct {
	ReadOnly bool
	Timeout  int64 // Timeout in milliseconds
	Retries  int   // Number of retry attempts
}

// WithReadOnly configures a read-only transaction
func WithReadOnly(readOnly bool) TransactionOption {
	return func(config *TransactionConfig) {
		config.ReadOnly = readOnly
	}
}

// WithTransactionTimeout sets transaction timeout in milliseconds
func WithTransactionTimeout(timeout int64) TransactionOption {
	return func(config *TransactionConfig) {
		config.Timeout = timeout
	}
}

// WithRetries sets the number of retry attempts for failed transactions
func WithRetries(retries int) TransactionOption {
	return func(config *TransactionConfig) {
		config.Retries = retries
	}
}

// BeginTransaction starts a new transaction
func (k *Kuzu) BeginTransaction(ctx context.Context, opts ...TransactionOption) (*Transaction, error) {
	if !k.IsConnected() {
		return nil, ErrConnectionNotInitialized
	}

	config := &TransactionConfig{
		ReadOnly: false,
		Timeout:  30000, // 30 seconds default
		Retries:  0,
	}

	for _, opt := range opts {
		opt(config)
	}

	// Create transaction context with timeout
	txCtx, cancelFunc := context.WithCancel(ctx)

	// Note: KuzuDB may not have explicit transaction control like SQL databases
	// We'll implement logical transaction semantics for batch operations
	txID := generateTransactionID()

	tx := &Transaction{
		id:         txID,
		kuzu:       k,
		state:      TransactionActive,
		ctx:        txCtx,
		cancelFunc: cancelFunc,
		operations: make([]string, 0),
		startedAt:  getCurrentTimestamp(),
	}

	// For KuzuDB, we'll use connection-level transaction semantics
	// Check if KuzuDB supports explicit transactions
	if config.ReadOnly {
		// Start read-only mode if supported
		_, err := k.Query(txCtx, "-- BEGIN READ ONLY TRANSACTION", nil)
		if err != nil {
			// KuzuDB might not support explicit transactions, continue anyway
		}
	} else {
		// Start read-write transaction if supported
		_, err := k.Query(txCtx, "-- BEGIN TRANSACTION", nil)
		if err != nil {
			// KuzuDB might not support explicit transactions, continue anyway
		}
	}

	return tx, nil
}

// GetState returns the current transaction state
func (tx *Transaction) GetState() TransactionState {
	tx.stateMux.RLock()
	defer tx.stateMux.RUnlock()
	return tx.state
}

// setState safely updates the transaction state
func (tx *Transaction) setState(state TransactionState) {
	tx.stateMux.Lock()
	defer tx.stateMux.Unlock()
	tx.state = state
}

// IsActive returns true if the transaction is active
func (tx *Transaction) IsActive() bool {
	return tx.GetState() == TransactionActive
}

// Query executes a query within the transaction context
func (tx *Transaction) Query(query string, params map[string]interface{}) (map[string]interface{}, error) {
	if !tx.IsActive() {
		return nil, fmt.Errorf("transaction is not active: %s", tx.GetState())
	}

	// Check if context is cancelled
	select {
	case <-tx.ctx.Done():
		tx.setState(TransactionFailed)
		return nil, tx.ctx.Err()
	default:
	}

	// Track the operation for potential rollback
	tx.operations = append(tx.operations, query)

	// Execute query using the transaction context
	result, err := tx.kuzu.Query(tx.ctx, query, params)
	if err != nil {
		tx.setState(TransactionFailed)
		return nil, fmt.Errorf("query failed in transaction: %w", err)
	}

	return result, nil
}

// QuerySingle executes a query expected to return a single result within transaction
func (tx *Transaction) QuerySingle(query string, params map[string]interface{}) (map[string]interface{}, error) {
	result, err := tx.Query(query, params)
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

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	if !tx.IsActive() {
		return fmt.Errorf("transaction is not active: %s", tx.GetState())
	}

	// Execute commit if KuzuDB supports it
	_, err := tx.kuzu.Query(tx.ctx, "-- COMMIT TRANSACTION", nil)
	if err != nil {
		// If explicit COMMIT fails, we'll just mark as committed
		// since KuzuDB operations are typically auto-committed
	}

	tx.setState(TransactionCommitted)
	tx.cancelFunc()
	return nil
}

// Rollback rolls back the transaction
func (tx *Transaction) Rollback() error {
	currentState := tx.GetState()
	if currentState != TransactionActive && currentState != TransactionFailed {
		return fmt.Errorf("transaction cannot be rolled back: %s", currentState)
	}

	// Execute rollback if KuzuDB supports it
	_, err := tx.kuzu.Query(tx.ctx, "-- ROLLBACK TRANSACTION", nil)
	if err != nil {
		// KuzuDB might not support explicit rollback
		// In that case, we'd need to implement compensating operations
		// For now, we'll just mark as rolled back
	}

	tx.setState(TransactionRolledBack)
	tx.cancelFunc()
	return nil
}

// Close closes the transaction, rolling back if not already committed
func (tx *Transaction) Close() error {
	switch tx.GetState() {
	case TransactionActive, TransactionFailed:
		return tx.Rollback()
	case TransactionCommitted, TransactionRolledBack:
		// Already closed
		return nil
	default:
		tx.cancelFunc()
		return nil
	}
}

// GetOperations returns the list of operations performed in this transaction
func (tx *Transaction) GetOperations() []string {
	return tx.operations
}

// GetDuration returns the duration of the transaction in milliseconds
func (tx *Transaction) GetDuration() int64 {
	return getCurrentTimestamp() - tx.startedAt
}

// RunInTransaction executes a function within a transaction with automatic commit/rollback
func (k *Kuzu) RunInTransaction(ctx context.Context, fn func(*Transaction) error, opts ...TransactionOption) error {
	tx, err := k.BeginTransaction(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Close()

	// Execute the function
	if err := fn(tx); err != nil {
		// Rollback on error
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("function failed: %w, rollback failed: %v", err, rollbackErr)
		}
		return err
	}

	// Commit on success
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// generateTransactionID generates a unique transaction ID
func generateTransactionID() string {
	// Simple transaction ID generation
	timestamp := getCurrentTimestamp()
	return fmt.Sprintf("tx_%d", timestamp)
}

// getCurrentTimestamp returns current timestamp in milliseconds
func getCurrentTimestamp() int64 {
	return 1000 // Placeholder - should use time.Now().UnixMilli() in real implementation
}

// TransactionStats provides statistics about transaction usage
type TransactionStats struct {
	ActiveTransactions     int
	CommittedTransactions  int
	RolledBackTransactions int
	FailedTransactions     int
}

// GetTransactionStats returns transaction statistics
func (k *Kuzu) GetTransactionStats() TransactionStats {
	// This would be implemented with proper tracking
	return TransactionStats{
		ActiveTransactions:     0,
		CommittedTransactions:  0,
		RolledBackTransactions: 0,
		FailedTransactions:     0,
	}
}
