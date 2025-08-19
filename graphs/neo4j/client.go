package neo4j

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// connect initializes the Neo4j driver connection
func (n *Neo4j) connect() error {
	if n.uri == "" {
		return ErrInvalidURI
	}

	// Create authentication token
	auth := neo4j.BasicAuth(n.username, n.password, "")

	// Create driver with context support
	driver, err := neo4j.NewDriverWithContext(n.uri, auth, func(config *neo4j.Config) {
		// Apply any custom configuration
		if n.config.MaxConnectionLifetime != 0 {
			config.MaxConnectionLifetime = n.config.MaxConnectionLifetime
		}
		if n.config.MaxConnectionPoolSize != 0 {
			config.MaxConnectionPoolSize = n.config.MaxConnectionPoolSize
		}
		if n.config.ConnectionAcquisitionTimeout != 0 {
			config.ConnectionAcquisitionTimeout = n.config.ConnectionAcquisitionTimeout
		}
	})

	if err != nil {
		return err
	}

	n.driver = driver

	// Verify connectivity
	ctx := context.Background()
	if err := n.driver.VerifyConnectivity(ctx); err != nil {
		n.driver.Close(ctx)
		return err
	}

	return nil
}

// Close closes the Neo4j driver connection
func (n *Neo4j) Close() error {
	if n.driver != nil {
		return n.driver.Close(context.Background())
	}
	return nil
}

// Query executes a Cypher query against the Neo4j database
func (n *Neo4j) Query(ctx context.Context, query string, params map[string]interface{}) (map[string]interface{}, error) {
	if n.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	// Create session
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: n.database,
	})
	defer session.Close(ctx)

	// Execute query with timeout
	var result neo4j.ResultWithContext
	var err error

	if n.timeout > 0 {
		// Create a context with timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, n.timeout)
		defer cancel()
		result, err = session.Run(timeoutCtx, query, params)
	} else {
		result, err = session.Run(ctx, query, params)
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrQueryExecution, err)
	}

	// Collect all records
	var records []map[string]interface{}
	for result.Next(ctx) {
		record := result.Record()
		records = append(records, record.AsMap())
	}

	// Check for errors during iteration
	if err = result.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrQueryExecution, err)
	}

	// Apply sanitization if enabled
	if n.sanitize {
		sanitizedRecords := make([]map[string]interface{}, 0, len(records))
		for _, record := range records {
			if sanitized := valueSanitize(record); sanitized != nil {
				if sanitizedMap, ok := sanitized.(map[string]interface{}); ok {
					sanitizedRecords = append(sanitizedRecords, sanitizedMap)
				}
			}
		}
		records = sanitizedRecords
	}

	return map[string]interface{}{
		"records": records,
		"summary": map[string]interface{}{
			"query":      query,
			"parameters": params,
		},
	}, nil
}
