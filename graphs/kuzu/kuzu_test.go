package kuzu

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tmc/langchaingo/graphs"
	"github.com/tmc/langchaingo/schema"
)

func TestKuzuNew(t *testing.T) {
	tests := []struct {
		name        string
		options     []Option
		expectError bool
	}{
		{
			name: "in-memory database with dangerous requests allowed",
			options: []Option{
				WithInMemory(true),
				WithAllowDangerousRequests(true),
			},
			expectError: false,
		},
		{
			name:        "dangerous requests disabled",
			options:     []Option{WithInMemory(true)},
			expectError: true,
		},
		{
			name: "file-based database with dangerous requests allowed",
			options: []Option{
				WithDatabasePath("./test_kuzu_db"),
				WithAllowDangerousRequests(true),
				WithTimeout(10 * time.Second),
				WithMaxNumThreads(2),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kuzu, err := NewKuzu(tt.options...)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			defer kuzu.Close()

			// Test connection health
			if !kuzu.IsConnected() {
				t.Error("Expected connection to be active")
			}

			// Test health check
			ctx := context.Background()
			if err := kuzu.HealthCheck(ctx); err != nil {
				t.Errorf("Health check failed: %v", err)
			}
		})
	}
}

func TestKuzuBasicOperations(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	ctx := context.Background()

	t.Run("basic query", func(t *testing.T) {
		result, err := kuzu.Query(ctx, "RETURN 1 as test_value", nil)
		if err != nil {
			t.Fatalf("Basic query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Result is nil")
		}

		summary, ok := result["summary"].(map[string]interface{})
		if !ok {
			t.Fatal("Summary not found in result")
		}

		if summary["query"] != "RETURN 1 as test_value" {
			t.Error("Query not properly recorded in summary")
		}
	})

	t.Run("query with parameters", func(t *testing.T) {
		params := map[string]interface{}{
			"test_param": "test_value",
		}

		result, err := kuzu.Query(ctx, "RETURN $test_param as param_value", params)
		if err != nil {
			t.Fatalf("Parameterized query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Result is nil")
		}
	})

	t.Run("query count", func(t *testing.T) {
		count, err := kuzu.QueryCount(ctx, "RETURN 1", nil)
		if err != nil {
			t.Fatalf("QueryCount failed: %v", err)
		}

		if count == 0 {
			t.Error("Expected count > 0")
		}
	})
}

func TestKuzuSchemaOperations(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	ctx := context.Background()

	t.Run("refresh schema", func(t *testing.T) {
		err := kuzu.RefreshSchema(ctx)
		if err != nil {
			t.Fatalf("RefreshSchema failed: %v", err)
		}

		schema := kuzu.GetSchema()
		if schema == "" {
			t.Error("Schema should not be empty after refresh")
		}
	})

	t.Run("structured schema", func(t *testing.T) {
		structuredSchema := kuzu.GetStructuredSchema()
		if structuredSchema == nil {
			t.Error("Structured schema should not be nil")
		}
	})

	t.Run("schema validation", func(t *testing.T) {
		err := kuzu.ValidateSchema(ctx)
		if err != nil {
			t.Logf("Schema validation info: %v", err)
		}
	})
}

func TestKuzuGraphDocumentOperations(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	ctx := context.Background()

	// Create test graph document
	doc := createTestGraphDocument()

	t.Run("add graph document", func(t *testing.T) {
		err := kuzu.AddGraphDocument(ctx, []graphs.GraphDocument{doc})
		if err != nil {
			t.Fatalf("AddGraphDocument failed: %v", err)
		}
	})

	t.Run("add multiple graph documents", func(t *testing.T) {
		docs := []graphs.GraphDocument{
			createTestGraphDocument(),
			createTestGraphDocument(),
		}

		err := kuzu.AddGraphDocuments(ctx, docs)
		if err != nil {
			t.Fatalf("AddGraphDocuments failed: %v", err)
		}
	})

	t.Run("add graph document with source", func(t *testing.T) {
		options := []graphs.Option{
			graphs.WithIncludeSource(true),
		}

		err := kuzu.AddGraphDocument(ctx, []graphs.GraphDocument{doc}, options...)
		if err != nil {
			t.Fatalf("AddGraphDocument with source failed: %v", err)
		}
	})
}

func TestKuzuTransactionOperations(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	ctx := context.Background()

	t.Run("basic transaction", func(t *testing.T) {
		tx, err := kuzu.BeginTransaction(ctx, WithReadOnly(false))
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Close()

		if !tx.IsActive() {
			t.Error("Transaction should be active")
		}

		// Execute query in transaction
		_, err = tx.Query("RETURN 1", nil)
		if err != nil {
			t.Fatalf("Transaction query failed: %v", err)
		}

		// Commit transaction
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Transaction commit failed: %v", err)
		}

		if tx.GetState() != TransactionCommitted {
			t.Error("Transaction should be committed")
		}
	})

	t.Run("transaction rollback", func(t *testing.T) {
		tx, err := kuzu.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Close()

		// Rollback transaction
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("Transaction rollback failed: %v", err)
		}

		if tx.GetState() != TransactionRolledBack {
			t.Error("Transaction should be rolled back")
		}
	})

	t.Run("run in transaction", func(t *testing.T) {
		err := kuzu.RunInTransaction(ctx, func(tx *Transaction) error {
			_, err := tx.Query("RETURN 1", nil)
			return err
		})

		if err != nil {
			t.Fatalf("RunInTransaction failed: %v", err)
		}
	})

	t.Run("transaction with graph document", func(t *testing.T) {
		tx, err := kuzu.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Close()

		doc := createTestGraphDocument()
		err = kuzu.AddGraphDocumentsWithTransaction(tx, []graphs.GraphDocument{doc})
		if err != nil {
			t.Fatalf("AddGraphDocumentsWithTransaction failed: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Transaction commit failed: %v", err)
		}
	})
}

func TestKuzuTypeConversion(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	ctx := context.Background()
	_ = ctx // Use ctx to avoid unused variable error

	t.Run("type converter", func(t *testing.T) {
		converter := kuzu.GetTypeConverter()
		if converter == nil {
			t.Error("Type converter should not be nil")
		}

		// Test basic type conversion
		testValues := map[string]interface{}{
			"string_value": "test",
			"int_value":    42,
			"float_value":  3.14,
			"bool_value":   true,
			"slice_value":  []interface{}{1, 2, 3},
			"map_value":    map[string]interface{}{"key": "value"},
		}

		for name, value := range testValues {
			t.Run(name, func(t *testing.T) {
				converted, dataType, err := converter.ConvertGoValueToKuzu(value)
				if err != nil {
					t.Fatalf("Type conversion failed for %s: %v", name, err)
				}

				if converted == nil && value != nil {
					t.Errorf("Converted value is nil for %s", name)
				}

				if dataType == "" {
					t.Errorf("Data type is empty for %s", name)
				}

				t.Logf("%s: %v -> %v (%s)", name, value, converted, dataType)
			})
		}
	})

	t.Run("property converter", func(t *testing.T) {
		converter := kuzu.GetPropertyConverter()
		if converter == nil {
			t.Error("Property converter should not be nil")
		}

		properties := map[string]interface{}{
			"name":     "test",
			"age":      25,
			"active":   true,
			"tags":     []string{"tag1", "tag2"},
			"metadata": map[string]interface{}{"type": "test"},
		}

		converted, err := converter.ConvertProperties(properties)
		if err != nil {
			t.Fatalf("Property conversion failed: %v", err)
		}

		if len(converted) != len(properties) {
			t.Error("Converted properties count mismatch")
		}
	})

	t.Run("validate property types", func(t *testing.T) {
		validProperties := map[string]interface{}{
			"name": "test",
			"age":  25,
		}

		err := kuzu.ValidatePropertyTypes(validProperties)
		if err != nil {
			t.Fatalf("Property validation failed: %v", err)
		}
	})
}

func TestKuzuErrorHandling(t *testing.T) {
	t.Run("connection errors", func(t *testing.T) {
		// Test with no dangerous requests
		_, err := NewKuzu(WithInMemory(true))
		if err == nil {
			t.Error("Expected error for dangerous requests disabled")
		}

		if err != ErrDangerousRequestsDisabled {
			t.Errorf("Expected ErrDangerousRequestsDisabled, got: %v", err)
		}
	})

	t.Run("query errors", func(t *testing.T) {
		kuzu := createTestKuzu(t)
		defer kuzu.Close()

		ctx := context.Background()

		// Test invalid query
		_, err := kuzu.Query(ctx, "INVALID CYPHER QUERY", nil)
		if err == nil {
			t.Error("Expected error for invalid query")
		}
		_ = ctx // Use ctx to avoid unused variable error
	})

	t.Run("transaction errors", func(t *testing.T) {
		kuzu := createTestKuzu(t)
		defer kuzu.Close()

		ctx := context.Background()

		tx, err := kuzu.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		// Commit first
		err = tx.Commit()
		if err != nil {
			t.Fatalf("First commit failed: %v", err)
		}

		// Try to commit again - should fail
		err = tx.Commit()
		if err == nil {
			t.Error("Expected error for double commit")
		}
	})
}

func TestKuzuPerformance(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	ctx := context.Background()

	t.Run("batch vs individual", func(t *testing.T) {
		// Create large graph document
		doc := createLargeGraphDocument(100)

		start := time.Now()
		err := kuzu.AddGraphDocument(ctx, []graphs.GraphDocument{doc})
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Large document import failed: %v", err)
		}

		t.Logf("Imported 100 nodes in %v", duration)

		// Test batch processing
		docs := make([]graphs.GraphDocument, 10)
		for i := range docs {
			docs[i] = createTestGraphDocument()
		}

		start = time.Now()
		err = kuzu.AddGraphDocuments(ctx, docs)
		duration = time.Since(start)

		if err != nil {
			t.Fatalf("Batch document import failed: %v", err)
		}

		t.Logf("Imported 10 documents in %v", duration)
	})

	t.Run("query with retry", func(t *testing.T) {
		result, err := kuzu.QueryWithRetry(ctx, "RETURN 1", nil, 3)
		if err != nil {
			t.Fatalf("QueryWithRetry failed: %v", err)
		}

		if result == nil {
			t.Error("Result should not be nil")
		}
	})
}

func TestKuzuUtilities(t *testing.T) {
	kuzu := createTestKuzu(t)
	defer kuzu.Close()

	t.Run("import statistics", func(t *testing.T) {
		stats := kuzu.GetImportStatistics()
		if stats == nil {
			t.Error("Import statistics should not be nil")
		}

		t.Logf("Import statistics: %+v", stats)
	})

	t.Run("deduplication", func(t *testing.T) {
		// Test node deduplication
		nodes := []graphs.Node{
			graphs.NewNode("1", "Person"),
			graphs.NewNode("1", "Person"), // Duplicate
			graphs.NewNode("2", "Person"),
		}

		deduplicated := kuzu.DeduplicateNodes(nodes)
		if len(deduplicated) != 2 {
			t.Errorf("Expected 2 nodes after deduplication, got %d", len(deduplicated))
		}

		// Test relationship deduplication
		node1 := graphs.NewNode("1", "Person")
		node2 := graphs.NewNode("2", "Person")

		rels := []graphs.Relationship{
			graphs.NewRelationship(node1, node2, "KNOWS"),
			graphs.NewRelationship(node1, node2, "KNOWS"), // Duplicate
			graphs.NewRelationship(node2, node1, "KNOWS"),
		}

		deduplicatedRels := kuzu.DeduplicateRelationships(rels)
		if len(deduplicatedRels) != 2 {
			t.Errorf("Expected 2 relationships after deduplication, got %d", len(deduplicatedRels))
		}
	})
}

// Helper functions

func createTestKuzu(t *testing.T) *Kuzu {
	t.Helper()

	kuzu, err := NewKuzu(
		WithInMemory(true),
		WithAllowDangerousRequests(true),
		WithTimeout(5*time.Second),
		WithMaxNumThreads(1),
	)

	if err != nil {
		t.Fatalf("Failed to create test Kuzu instance: %v", err)
	}

	return kuzu
}

func createTestGraphDocument() graphs.GraphDocument {
	doc := schema.Document{
		PageContent: "Test document content about Alice and Bob",
		Metadata: map[string]interface{}{
			"source": "test",
			"id":     "test-doc-1",
		},
	}

	graphDoc := graphs.NewGraphDocument(doc)

	// Add test nodes
	node1 := graphs.NewNode("alice", "Person")
	node1.SetProperty("name", "Alice")
	node1.SetProperty("age", 30)
	node1.SetProperty("active", true)
	graphDoc.AddNode(node1)

	node2 := graphs.NewNode("bob", "Person")
	node2.SetProperty("name", "Bob")
	node2.SetProperty("age", 25)
	node2.SetProperty("active", false)
	graphDoc.AddNode(node2)

	// Add test relationship
	rel := graphs.NewRelationship(node1, node2, "KNOWS")
	rel.SetProperty("since", "2020")
	rel.SetProperty("strength", 0.8)
	graphDoc.AddRelationship(rel)

	return graphDoc
}

func createLargeGraphDocument(nodeCount int) graphs.GraphDocument {
	doc := schema.Document{
		PageContent: "Large test document",
		Metadata: map[string]interface{}{
			"source": "large-test",
			"id":     "large-doc-1",
		},
	}

	graphDoc := graphs.NewGraphDocument(doc)

	// Create many nodes
	nodes := make([]graphs.Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		node := graphs.NewNode(fmt.Sprintf("node_%d", i), "TestNode")
		node.SetProperty("index", i)
		node.SetProperty("name", fmt.Sprintf("Node %d", i))
		nodes[i] = node
		graphDoc.AddNode(node)
	}

	// Create relationships between adjacent nodes
	for i := 0; i < nodeCount-1; i++ {
		rel := graphs.NewRelationship(nodes[i], nodes[i+1], "CONNECTS")
		rel.SetProperty("order", i)
		graphDoc.AddRelationship(rel)
	}

	return graphDoc
}
