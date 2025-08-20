// Package neo4j provides a wrapper around the Neo4j vector search capabilities for LangChain Go.
// It implements the VectorStore interface to store and retrieve documents using Neo4j's
// vector indexes for semantic similarity search.
//
// The Neo4j vector store leverages Neo4j's built-in vector index capabilities to enable
// efficient approximate nearest neighbor (ANN) searches on document embeddings.
//
// Neo4j supports vector indexing with both COSINE and EUCLIDEAN similarity functions.
// The store automatically creates and manages vector indexes as needed.
//
// Basic usage:
//
//	store := neo4j.New(
//		neo4j.WithConnectionURL("bolt://localhost:7687"),
//		neo4j.WithCredentials("neo4j", "password"),
//		neo4j.WithEmbedder(embedder),
//		neo4j.WithIndexName("documents"),
//		neo4j.WithNodeLabel("Document"),
//	)
//
//	// Add documents
//	ids, err := store.AddDocuments(ctx, documents)
//
//	// Search for similar documents
//	results, err := store.SimilaritySearch(ctx, "search query", 5)
package neo4j
