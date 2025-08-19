package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tmc/langchaingo/graphs"
	"github.com/tmc/langchaingo/graphs/neo4j"
	"github.com/tmc/langchaingo/schema"
)

func main() {
	// Create a Neo4j GraphStore instance with various options
	store, err := neo4j.NewNeo4j(
		// Connection settings - can also be set via environment variables:
		// NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
		neo4j.WithURI("bolt://localhost:7687"),
		neo4j.WithAuth("neo4j", "password"),
		neo4j.WithDatabase("neo4j"),

		// Performance and behavior options
		neo4j.WithSanitize(true),          // Remove large lists/embeddings from results
		neo4j.WithEnhancedSchema(true),    // Include example values in schema
		neo4j.WithBaseEntityLabel(false),  // Use __Entity__ label for performance
		neo4j.WithTimeout(30*time.Second), // Query timeout
	)
	if err != nil {
		log.Fatalf("Failed to create Neo4j store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create a sample document
	doc := schema.Document{
		PageContent: "Alice works at ACME Corp and knows Bob who works at XYZ Inc.",
		Metadata: map[string]interface{}{
			"source":   "example",
			"category": "professional",
		},
	}

	// Create a graph document with nodes and relationships
	graphDoc := graphs.NewGraphDocument(doc)

	// Add person nodes
	alice := graphs.NewNode("alice", "Person")
	alice.SetProperty("name", "Alice")
	alice.SetProperty("age", 30)
	graphDoc.AddNode(alice)

	bob := graphs.NewNode("bob", "Person")
	bob.SetProperty("name", "Bob")
	bob.SetProperty("age", 25)
	graphDoc.AddNode(bob)

	// Add company nodes
	acme := graphs.NewNode("acme", "Company")
	acme.SetProperty("name", "ACME Corp")
	acme.SetProperty("industry", "Technology")
	graphDoc.AddNode(acme)

	xyz := graphs.NewNode("xyz", "Company")
	xyz.SetProperty("name", "XYZ Inc")
	xyz.SetProperty("industry", "Finance")
	graphDoc.AddNode(xyz)

	// Add relationships
	worksAt1 := graphs.NewRelationship(alice, acme, "WORKS_AT")
	worksAt1.SetProperty("since", "2020")
	graphDoc.AddRelationship(worksAt1)

	worksAt2 := graphs.NewRelationship(bob, xyz, "WORKS_AT")
	worksAt2.SetProperty("since", "2021")
	graphDoc.AddRelationship(worksAt2)

	knows := graphs.NewRelationship(alice, bob, "KNOWS")
	knows.SetProperty("relationship", "colleague")
	graphDoc.AddRelationship(knows)

	// Import the graph document
	fmt.Println("Importing graph document...")
	err = store.AddGraphDocument(ctx, []graphs.GraphDocument{graphDoc},
		graphs.WithIncludeSource(true), // Link nodes to source document
		graphs.WithBatchSize(100),      // Process in batches
	)
	if err != nil {
		log.Fatalf("Failed to import graph document: %v", err)
	}
	fmt.Println("✓ Graph document imported successfully")

	// Refresh and display schema
	fmt.Println("\nRefreshing schema...")
	err = store.RefreshSchema(ctx)
	if err != nil {
		log.Fatalf("Failed to refresh schema: %v", err)
	}

	schema := store.GetSchema()
	fmt.Println("Graph Schema:")
	fmt.Println(schema)

	// Get structured schema for programmatic access
	structuredSchema := store.GetStructuredSchema()
	fmt.Printf("\nStructured schema contains %d node types and %d relationship types\n",
		len(structuredSchema["node_props"].(map[string]interface{})),
		len(structuredSchema["rel_props"].(map[string]interface{})))

	// Execute some sample queries
	fmt.Println("\nExecuting sample queries...")

	// Find all people
	peopleResult, err := store.Query(ctx, "MATCH (p:Person) RETURN p.name as name, p.age as age", nil)
	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		fmt.Println("People in the graph:")
		if records, ok := peopleResult["records"].([]map[string]interface{}); ok {
			for _, record := range records {
				fmt.Printf("  - %s (age: %v)\n", record["name"], record["age"])
			}
		}
	}

	// Find relationships
	relResult, err := store.Query(ctx, `
		MATCH (a:Person)-[r]->(b) 
		RETURN a.name as from, type(r) as relationship, b.name as to
	`, nil)
	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		fmt.Println("\nRelationships:")
		if records, ok := relResult["records"].([]map[string]interface{}); ok {
			for _, record := range records {
				fmt.Printf("  %s -[%s]-> %s\n", record["from"], record["relationship"], record["to"])
			}
		}
	}

	// Complex query with parameters
	companyQuery := `
		MATCH (p:Person)-[:WORKS_AT]->(c:Company)
		WHERE c.industry = $industry
		RETURN p.name as employee, c.name as company
	`
	techResult, err := store.Query(ctx, companyQuery, map[string]interface{}{
		"industry": "Technology",
	})
	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		fmt.Println("\nTechnology company employees:")
		if records, ok := techResult["records"].([]map[string]interface{}); ok {
			for _, record := range records {
				fmt.Printf("  %s works at %s\n", record["employee"], record["company"])
			}
		}
	}

	fmt.Println("\n✓ Example completed successfully")
}
