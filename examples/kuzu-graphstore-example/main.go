package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tmc/langchaingo/graphs"
	"github.com/tmc/langchaingo/graphs/kuzu"
	"github.com/tmc/langchaingo/schema"
)

func main() {
	fmt.Println("KuzuDB GraphStore Example")
	fmt.Println("=========================")

	// Example 1: In-Memory Database
	fmt.Println("\n1. Creating in-memory KuzuDB instance...")
	inMemoryExample()

	// Example 2: File-Based Database
	fmt.Println("\n2. Creating file-based KuzuDB instance...")
	fileBasedExample()

	// Example 3: Advanced Features
	fmt.Println("\n3. Demonstrating advanced features...")
	advancedFeaturesExample()

	fmt.Println("\nExample completed successfully!")
}

func inMemoryExample() {
	// Create a KuzuDB GraphStore instance with in-memory database
	store, err := kuzu.NewKuzu(
		// Database configuration
		kuzu.WithInMemory(true),               // Use in-memory database for this example
		kuzu.WithAllowDangerousRequests(true), // Required for KuzuDB operations

		// Performance options
		kuzu.WithMaxNumThreads(2),              // Use 2 threads for queries
		kuzu.WithTimeout(30*time.Second),       // Query timeout
		kuzu.WithBufferPoolSize(512*1024*1024), // 512MB buffer pool
		kuzu.WithEnableLogging(true),           // Enable logging
		kuzu.WithLogLevel("info"),              // Set log level
	)
	if err != nil {
		log.Fatalf("Failed to create KuzuDB store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Test connection health
	if err := store.HealthCheck(ctx); err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	fmt.Println("✓ Connection established and healthy")

	// Create sample documents with graph data
	doc1 := schema.Document{
		PageContent: "Alice works at ACME Corp and knows Bob who works at XYZ Inc. They collaborated on Project Alpha.",
		Metadata: map[string]interface{}{
			"source":   "company_directory",
			"category": "professional",
			"id":       "doc1",
		},
	}

	doc2 := schema.Document{
		PageContent: "Bob and Charlie are colleagues at XYZ Inc. Charlie leads the engineering team.",
		Metadata: map[string]interface{}{
			"source":   "company_directory",
			"category": "professional",
			"id":       "doc2",
		},
	}

	// Create graph documents with nodes and relationships
	graphDoc1 := createGraphDocument1(doc1)
	graphDoc2 := createGraphDocument2(doc2)

	// Import graph documents
	fmt.Println("Importing graph documents...")

	// Import with source tracking
	err = store.AddGraphDocument(ctx, []graphs.GraphDocument{graphDoc1},
		graphs.WithIncludeSource(true))
	if err != nil {
		log.Fatalf("Failed to import first graph document: %v", err)
	}
	fmt.Println("✓ Imported first graph document with source tracking")

	// Import multiple documents at once
	err = store.AddGraphDocuments(ctx, []graphs.GraphDocument{graphDoc2})
	if err != nil {
		log.Fatalf("Failed to import second graph document: %v", err)
	}
	fmt.Println("✓ Imported second graph document")

	// Refresh and display schema
	fmt.Println("\nRefreshing schema...")
	err = store.RefreshSchema(ctx)
	if err != nil {
		log.Fatalf("Failed to refresh schema: %v", err)
	}

	schema := store.GetSchema()
	fmt.Printf("Database Schema:\n%s\n", schema)

	// Query the data
	fmt.Println("Querying data...")

	// Find all people - use map_extract to get properties from MAP
	result, err := store.Query(ctx, "MATCH (p:Person) RETURN element_at(p.properties, 'name') as name, element_at(p.properties, 'age') as age", nil)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	displayQueryResult("All People", result)

	// Find relationships - use map_extract for both node and relationship properties
	result, err = store.Query(ctx,
		"MATCH (p1:Person)-[r:KNOWS]->(p2:Person) RETURN element_at(p1.properties, 'name') as person1, element_at(p2.properties, 'name') as person2, element_at(r.properties, 'since') as since",
		nil)
	if err != nil {
		log.Fatalf("Relationship query failed: %v", err)
	}
	displayQueryResult("Relationships", result)

	// Get import statistics
	// TODO: Implement GetImportStatistics method
	// stats := store.GetImportStatistics()
	// fmt.Printf("Import Statistics: %+v\n", stats)
}

func fileBasedExample() {
	// Create a file-based KuzuDB instance
	store, err := kuzu.NewKuzu(
		kuzu.WithDatabasePath("./example_kuzu_db"), // File-based database
		kuzu.WithAllowDangerousRequests(true),
		kuzu.WithTimeout(30*time.Second),
		kuzu.WithMaxNumThreads(4),
	)
	if err != nil {
		log.Fatalf("Failed to create file-based KuzuDB store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	fmt.Println("✓ File-based database created")

	// Create a more complex graph with companies and projects
	doc := schema.Document{
		PageContent: "TechCorp acquired StartupInc for $100M. The acquisition includes their AI team and Project Beta.",
		Metadata: map[string]interface{}{
			"source":   "business_news",
			"category": "mergers_acquisitions",
			"date":     "2024-01-15",
			"amount":   100000000,
		},
	}

	graphDoc := createAcquisitionGraphDocument(doc)

	// Use transaction for complex import
	fmt.Println("Using transaction for import...")
	err = store.RunInTransaction(ctx, func(tx *kuzu.Transaction) error {
		return store.AddGraphDocumentsWithTransaction(tx, []graphs.GraphDocument{graphDoc})
	})
	if err != nil {
		log.Fatalf("Transaction import failed: %v", err)
	}
	fmt.Println("✓ Complex graph imported using transaction")

	// Query with type conversion - use element_at for MAP properties
	result, err := store.QueryWithTypes(ctx,
		"MATCH (c:Company) RETURN element_at(c.properties, 'name') as company, element_at(c.properties, 'value') as value, element_at(c.properties, 'founded') as founded",
		nil)
	if err != nil {
		log.Fatalf("Typed query failed: %v", err)
	}
	displayQueryResult("Companies", result)

	// Test prepared statements
	fmt.Println("\nTesting prepared statements...")
	stmt, err := store.PrepareQuery("MATCH (c:Company) WHERE element_at(c.properties, 'name')[1] = $name RETURN element_at(c.properties, 'value') as value")
	if err != nil {
		log.Fatalf("Prepare query failed: %v", err)
	}
	defer stmt.Close()

	result, err = store.ExecutePreparedQuery(ctx, stmt, map[string]interface{}{
		"name": "TechCorp",
	})
	if err != nil {
		log.Fatalf("Prepared query execution failed: %v", err)
	}
	displayQueryResult("TechCorp Value", result)
}

func advancedFeaturesExample() {
	store, err := kuzu.NewKuzu(
		kuzu.WithInMemory(true),
		kuzu.WithAllowDangerousRequests(true),
	)
	if err != nil {
		log.Fatalf("Failed to create KuzuDB store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Demonstrate type conversion
	fmt.Println("Testing type conversions...")
	typeConverter := store.GetTypeConverter()

	testValues := map[string]interface{}{
		"string":  "Hello World",
		"integer": 42,
		"float":   3.14159,
		"boolean": true,
		"array":   []interface{}{1, 2, 3, "four"},
		"object":  map[string]interface{}{"nested": "value", "count": 5},
		"date":    time.Now(),
	}

	for name, value := range testValues {
		converted, dataType, err := typeConverter.ConvertGoValueToKuzu(value)
		if err != nil {
			fmt.Printf("✗ %s conversion failed: %v\n", name, err)
		} else {
			fmt.Printf("✓ %s: %v -> %v (%s)\n", name, value, converted, dataType)
		}
	}

	// Demonstrate property validation
	fmt.Println("\nValidating property types...")
	properties := map[string]interface{}{
		"name":     "Test Entity",
		"count":    100,
		"active":   true,
		"metadata": map[string]interface{}{"key": "value"},
	}

	err = store.ValidatePropertyTypes(properties)
	if err != nil {
		fmt.Printf("✗ Property validation failed: %v\n", err)
	} else {
		fmt.Println("✓ All properties are valid")
	}

	// Demonstrate schema utilities
	fmt.Println("\nTesting schema utilities...")

	// Create some test data first
	doc := createSimpleGraphDocument()
	err = store.AddGraphDocument(ctx, []graphs.GraphDocument{doc})
	if err != nil {
		log.Fatalf("Failed to add test document: %v", err)
	}

	// Get table list
	tables, err := store.GetTableList(ctx)
	if err != nil {
		fmt.Printf("✗ Failed to get table list: %v\n", err)
	} else {
		fmt.Printf("✓ Database tables: %v\n", tables)
	}

	// Check if table exists
	exists, err := store.TableExists(ctx, "Person")
	if err != nil {
		fmt.Printf("✗ Failed to check table existence: %v\n", err)
	} else {
		fmt.Printf("✓ Person table exists: %v\n", exists)
	}

	// Get schema version
	version := store.GetSchemaVersion()
	fmt.Printf("✓ Schema version: %s\n", version)

	// Demonstrate deduplication
	fmt.Println("\nTesting deduplication...")

	// Create duplicate nodes
	nodes := []graphs.Node{
		graphs.NewNode("test1", "TestNode"),
		graphs.NewNode("test1", "TestNode"), // Duplicate
		graphs.NewNode("test2", "TestNode"),
	}

	// TODO: Implement DeduplicateNodes method
	// deduplicated := store.DeduplicateNodes(nodes)
	// fmt.Printf("✓ Deduplicated %d nodes to %d unique nodes\n", len(nodes), len(deduplicated))
	fmt.Printf("✓ Created %d nodes (deduplication TODO)\n", len(nodes))

	// Performance test with retry
	fmt.Println("\nTesting query with retry...")
	start := time.Now()
	result, err := store.QueryWithRetry(ctx, "RETURN 1 as test", nil, 3)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("✗ Query with retry failed: %v\n", err)
	} else {
		fmt.Printf("✓ Query with retry completed in %v\n", duration)
		displayQueryResult("Retry Test", result)
	}
}

// Helper functions to create graph documents

func createGraphDocument1(doc schema.Document) graphs.GraphDocument {
	graphDoc := graphs.NewGraphDocument(doc)

	// Add person nodes
	alice := graphs.NewNode("alice", "Person")
	alice.SetProperty("name", "Alice")
	alice.SetProperty("age", 30)
	alice.SetProperty("department", "Engineering")
	graphDoc.AddNode(alice)

	bob := graphs.NewNode("bob", "Person")
	bob.SetProperty("name", "Bob")
	bob.SetProperty("age", 28)
	bob.SetProperty("department", "Marketing")
	graphDoc.AddNode(bob)

	// Add company nodes
	acme := graphs.NewNode("acme", "Company")
	acme.SetProperty("name", "ACME Corp")
	acme.SetProperty("industry", "Technology")
	acme.SetProperty("employees", 1000)
	graphDoc.AddNode(acme)

	xyz := graphs.NewNode("xyz", "Company")
	xyz.SetProperty("name", "XYZ Inc")
	xyz.SetProperty("industry", "Consulting")
	xyz.SetProperty("employees", 500)
	graphDoc.AddNode(xyz)

	// Add project node
	project := graphs.NewNode("alpha", "Project")
	project.SetProperty("name", "Project Alpha")
	project.SetProperty("status", "completed")
	project.SetProperty("budget", 50000)
	graphDoc.AddNode(project)

	// Add relationships
	worksAt1 := graphs.NewRelationship(alice, acme, "WORKS_AT")
	worksAt1.SetProperty("since", "2020-01-15")
	worksAt1.SetProperty("position", "Senior Engineer")
	graphDoc.AddRelationship(worksAt1)

	worksAt2 := graphs.NewRelationship(bob, xyz, "WORKS_AT")
	worksAt2.SetProperty("since", "2021-03-01")
	worksAt2.SetProperty("position", "Marketing Manager")
	graphDoc.AddRelationship(worksAt2)

	knows := graphs.NewRelationship(alice, bob, "KNOWS")
	knows.SetProperty("since", "2019-06-01")
	knows.SetProperty("strength", 0.8)
	graphDoc.AddRelationship(knows)

	collaborates := graphs.NewRelationship(alice, project, "WORKED_ON")
	collaborates.SetProperty("role", "Lead Developer")
	collaborates.SetProperty("hours", 320)
	graphDoc.AddRelationship(collaborates)

	return graphDoc
}

func createGraphDocument2(doc schema.Document) graphs.GraphDocument {
	graphDoc := graphs.NewGraphDocument(doc)

	// Add person nodes
	bob := graphs.NewNode("bob", "Person")
	bob.SetProperty("name", "Bob")
	bob.SetProperty("age", 28)
	graphDoc.AddNode(bob)

	charlie := graphs.NewNode("charlie", "Person")
	charlie.SetProperty("name", "Charlie")
	charlie.SetProperty("age", 35)
	charlie.SetProperty("department", "Engineering")
	graphDoc.AddNode(charlie)

	// Add relationships
	colleagues := graphs.NewRelationship(bob, charlie, "COLLEAGUE")
	colleagues.SetProperty("team", "Engineering")
	colleagues.SetProperty("since", "2021-03-01")
	graphDoc.AddRelationship(colleagues)

	reports := graphs.NewRelationship(bob, charlie, "REPORTS_TO")
	reports.SetProperty("since", "2022-01-01")
	graphDoc.AddRelationship(reports)

	return graphDoc
}

func createAcquisitionGraphDocument(doc schema.Document) graphs.GraphDocument {
	graphDoc := graphs.NewGraphDocument(doc)

	// Companies
	techCorp := graphs.NewNode("techcorp", "Company")
	techCorp.SetProperty("name", "TechCorp")
	techCorp.SetProperty("industry", "Technology")
	techCorp.SetProperty("value", 5000000000) // $5B
	techCorp.SetProperty("founded", "2010-05-15")
	graphDoc.AddNode(techCorp)

	startupInc := graphs.NewNode("startupinc", "Company")
	startupInc.SetProperty("name", "StartupInc")
	startupInc.SetProperty("industry", "AI/ML")
	startupInc.SetProperty("value", 100000000) // $100M
	startupInc.SetProperty("founded", "2018-08-20")
	graphDoc.AddNode(startupInc)

	// Project
	projectBeta := graphs.NewNode("beta", "Project")
	projectBeta.SetProperty("name", "Project Beta")
	projectBeta.SetProperty("type", "AI Research")
	projectBeta.SetProperty("status", "active")
	graphDoc.AddNode(projectBeta)

	// Team
	aiTeam := graphs.NewNode("ai_team", "Team")
	aiTeam.SetProperty("name", "AI Research Team")
	aiTeam.SetProperty("size", 15)
	aiTeam.SetProperty("specialization", "Machine Learning")
	graphDoc.AddNode(aiTeam)

	// Relationships
	acquisition := graphs.NewRelationship(techCorp, startupInc, "ACQUIRED")
	acquisition.SetProperty("amount", 100000000)
	acquisition.SetProperty("date", "2024-01-15")
	acquisition.SetProperty("currency", "USD")
	graphDoc.AddRelationship(acquisition)

	owns := graphs.NewRelationship(startupInc, projectBeta, "OWNS")
	owns.SetProperty("since", "2018-08-20")
	graphDoc.AddRelationship(owns)

	manages := graphs.NewRelationship(aiTeam, projectBeta, "MANAGES")
	manages.SetProperty("responsibility", "full")
	graphDoc.AddRelationship(manages)

	belongsTo := graphs.NewRelationship(aiTeam, startupInc, "BELONGS_TO")
	belongsTo.SetProperty("retained", true)
	graphDoc.AddRelationship(belongsTo)

	return graphDoc
}

func createSimpleGraphDocument() graphs.GraphDocument {
	doc := schema.Document{
		PageContent: "Simple test document",
		Metadata: map[string]interface{}{
			"source": "test",
		},
	}

	graphDoc := graphs.NewGraphDocument(doc)

	node := graphs.NewNode("test", "Person")
	node.SetProperty("name", "Test Person")
	graphDoc.AddNode(node)

	return graphDoc
}

// Utility functions

func displayQueryResult(title string, result map[string]interface{}) {
	fmt.Printf("\n%s:\n", title)
	fmt.Printf("----------------------------------------\n")

	if records, ok := result["records"].([]map[string]interface{}); ok {
		if len(records) == 0 {
			fmt.Println("No results found")
			return
		}

		for i, record := range records {
			fmt.Printf("Record %d:\n", i+1)
			for key, value := range record {
				fmt.Printf("  %s: %v\n", key, value)
			}
			fmt.Println()
		}

		if summary, ok := result["summary"].(map[string]interface{}); ok {
			if execTime, ok := summary["execution_time"].(float64); ok {
				fmt.Printf("Execution time: %.2fms\n", execTime)
			}
			if rowCount, ok := summary["number_of_rows"].(uint64); ok {
				fmt.Printf("Rows returned: %d\n", rowCount)
			}
		}
	} else {
		fmt.Printf("Result: %+v\n", result)
	}
	fmt.Println()
}
