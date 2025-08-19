package neo4j

import (
	"testing"

	"github.com/tmc/langchaingo/graphs"
	"github.com/tmc/langchaingo/schema"
)

func TestNeo4jNew(t *testing.T) {
	// Test creating a new Neo4j instance with options
	neo4j, err := NewNeo4j(
		WithURI("bolt://localhost:7687"),
		WithAuth("neo4j", "password"),
		WithDatabase("neo4j"),
		WithSanitize(true),
		WithEnhancedSchema(true),
		WithBaseEntityLabel(false),
	)

	if err != nil {
		// Expected to fail without a real Neo4j instance
		t.Logf("Expected connection failure: %v", err)
		return
	}

	defer neo4j.Close()

	// This would only work with a real Neo4j instance
	t.Log("Neo4j instance created successfully")
}

func TestNeo4jGraphDocument(t *testing.T) {
	// Test GraphDocument creation without actual database connection
	doc := schema.Document{
		PageContent: "Test document content",
		Metadata: map[string]interface{}{
			"source": "test",
		},
	}

	graphDoc := graphs.NewGraphDocument(doc)

	// Add some test nodes
	node1 := graphs.NewNode("1", "Person")
	node1.SetProperty("name", "Alice")
	graphDoc.AddNode(node1)

	node2 := graphs.NewNode("2", "Person")
	node2.SetProperty("name", "Bob")
	graphDoc.AddNode(node2)

	// Add a relationship
	rel := graphs.NewRelationship(node1, node2, "KNOWS")
	rel.SetProperty("since", "2020")
	graphDoc.AddRelationship(rel)

	if len(graphDoc.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(graphDoc.Nodes))
	}

	if len(graphDoc.Relationships) != 1 {
		t.Errorf("Expected 1 relationship, got %d", len(graphDoc.Relationships))
	}
}

func TestValueSanitize(t *testing.T) {
	// Test value sanitization function
	testData := map[string]interface{}{
		"normal_field": "value",
		"large_list":   make([]interface{}, 200), // Should be removed
		"small_list":   []interface{}{1, 2, 3},   // Should be kept
		"nested": map[string]interface{}{
			"inner_field": "inner_value",
			"large_inner": make([]interface{}, 150), // Should be removed
		},
	}

	sanitized := valueSanitize(testData)

	if sanitizedMap, ok := sanitized.(map[string]interface{}); ok {
		// large_list should be removed
		if _, exists := sanitizedMap["large_list"]; exists {
			t.Error("large_list should have been removed by sanitization")
		}

		// small_list should be kept
		if _, exists := sanitizedMap["small_list"]; !exists {
			t.Error("small_list should have been kept by sanitization")
		}

		// Check nested structure
		if nested, exists := sanitizedMap["nested"].(map[string]interface{}); exists {
			if _, innerExists := nested["large_inner"]; innerExists {
				t.Error("nested large_inner should have been removed by sanitization")
			}
		}
	} else {
		t.Error("Sanitized result should be a map")
	}
}

func TestCleanStringValues(t *testing.T) {
	input := "Line 1\nLine 2\rLine 3"
	expected := "Line 1 Line 2 Line 3"
	result := cleanStringValues(input)

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestAPOCErrorDetection(t *testing.T) {
	// Test APOC error detection
	normalError := "Some other error"
	apocError := "Neo.ClientError.Procedure.ProcedureNotFound: apoc.meta.data not found"

	if isAPOCError(nil) {
		t.Error("nil should not be detected as APOC error")
	}

	if isAPOCError(&TestError{normalError}) {
		t.Error("Normal error should not be detected as APOC error")
	}

	if !isAPOCError(&TestError{apocError}) {
		t.Error("APOC error should be detected")
	}
}

// TestError is a simple error implementation for testing
type TestError struct {
	message string
}

func (e *TestError) Error() string {
	return e.message
}
