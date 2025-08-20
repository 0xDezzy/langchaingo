// Package kuzu provides a graph store implementation for KuzuDB.
//
// KuzuDB is an embedded graph database management system built for query speed and scalability.
// It supports Cypher query language and provides high-performance graph operations through
// columnar storage and vectorized processing.
//
// This package implements the graphs.GraphStore interface, allowing integration with
// LangChain Go for graph-based AI applications.
//
// Features:
//   - Embedded database (no external server required)
//   - In-memory and file-based storage modes
//   - Full Cypher query language support
//   - High-performance columnar storage
//   - Transaction support with ACID properties
//   - Schema introspection and management
//   - Batch processing for large graph imports
//
// Example usage:
//
//	// Create an in-memory KuzuDB instance
//	store, err := kuzu.NewKuzu(
//		kuzu.WithInMemory(true),
//		kuzu.WithAllowDangerousRequests(true),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer store.Close()
//
//	// Create and import graph documents
//	doc := schema.Document{PageContent: "Sample text"}
//	graphDoc := graphs.NewGraphDocument(doc)
//
//	// Add nodes and relationships
//	alice := graphs.NewNode("alice", "Person")
//	bob := graphs.NewNode("bob", "Person")
//	rel := graphs.NewRelationship(alice, bob, "KNOWS")
//
//	graphDoc.AddNode(alice)
//	graphDoc.AddNode(bob)
//	graphDoc.AddRelationship(rel)
//
//	// Import to database
//	err = store.AddGraphDocument(ctx, []graphs.GraphDocument{graphDoc})
//	if err != nil {
//		log.Fatal(err)
//	}
//
// Security Note:
// KuzuDB supports arbitrary Cypher queries which can be powerful but potentially dangerous.
// Always validate user input and consider using the AllowDangerousRequests option carefully
// in production environments.
package kuzu
