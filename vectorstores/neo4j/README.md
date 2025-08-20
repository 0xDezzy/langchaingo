# Neo4j Vector Store

The Neo4j vector store provides a vector database implementation using Neo4j's native vector indexing capabilities. This implementation leverages Neo4j's HNSW (Hierarchical Navigable Small World) algorithm for efficient approximate nearest neighbor (ANN) searches.

## Features

- **Vector Similarity Search**: Efficient similarity search using Neo4j vector indexes
- **Hybrid Search**: Combine vector similarity with keyword search using reciprocal rank fusion (RRF)
- **Metadata Filtering**: Filter search results based on document metadata
- **Namespace Support**: Organize documents using namespaces
- **Multiple Similarity Functions**: Support for cosine and Euclidean similarity
- **Automatic Index Management**: Automatically create and manage vector indexes
- **Deduplication**: Built-in support for document deduplication

## Prerequisites

- **Neo4j 5.11+**: Vector indexes were introduced in Neo4j 5.11
- **APOC Plugin**: Required for some advanced operations (usually pre-installed in Neo4j)

## Quick Start

```go
package main

import (
    "context"
    "log"
    
    "github.com/tmc/langchaingo/embeddings"
    "github.com/tmc/langchaingo/llms/openai"
    "github.com/tmc/langchaingo/schema"
    "github.com/tmc/langchaingo/vectorstores"
    "github.com/tmc/langchaingo/vectorstores/neo4j"
)

func main() {
    ctx := context.Background()

    // Create embeddings provider
    llm, _ := openai.New()
    embedder, _ := embeddings.NewEmbedder(llm)

    // Create Neo4j vector store
    store, err := neo4j.New(ctx,
        neo4j.WithConnectionURL("bolt://localhost:7687"),
        neo4j.WithCredentials("neo4j", "password"),
        neo4j.WithEmbedder(embedder),
        neo4j.WithIndexName("documents"),
        neo4j.WithDimensions(1536), // OpenAI ada-002 dimensions
    )
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Add documents
    docs := []schema.Document{
        {
            PageContent: "Your document content here",
            Metadata: map[string]interface{}{
                "source": "example",
                "category": "documentation",
            },
        },
    }
    
    ids, err := store.AddDocuments(ctx, docs)
    if err != nil {
        log.Fatal(err)
    }

    // Search for similar documents
    results, err := store.SimilaritySearch(ctx, "search query", 5)
    if err != nil {
        log.Fatal(err)
    }

    // Process results
    for _, doc := range results {
        log.Printf("Score: %.4f - %s", doc.Score, doc.PageContent)
    }
}
```

## Configuration Options

### Connection Options

```go
store, err := neo4j.New(ctx,
    neo4j.WithConnectionURL("bolt://localhost:7687"),
    neo4j.WithCredentials("username", "password"),
    neo4j.WithDatabase("neo4j"), // Default database
)
```

### Vector Index Options

```go
store, err := neo4j.New(ctx,
    neo4j.WithIndexName("my_vector_index"),
    neo4j.WithDimensions(1536),
    neo4j.WithSimilarityFunction("cosine"), // or "euclidean"
    neo4j.WithCreateIndex(true), // Auto-create index
    neo4j.WithPreDeleteIndex(false), // Clean existing index
)
```

### Schema Options

```go
store, err := neo4j.New(ctx,
    neo4j.WithNodeLabel("Document"),
    neo4j.WithEmbeddingProperty("embedding"),
    neo4j.WithTextProperty("text"),
    neo4j.WithMetadataProperty("metadata"),
    neo4j.WithIDProperty("id"),
)
```

### Hybrid Search Options

```go
store, err := neo4j.New(ctx,
    neo4j.WithHybridSearch(true),
    neo4j.WithSearchType("hybrid"),
    neo4j.WithKeywordIndexName("keyword_index"),
    neo4j.WithRRF(map[string]interface{}{"k": 60}),
)
```

## Search Options

### Basic Similarity Search

```go
results, err := store.SimilaritySearch(ctx, "query", 10)
```

### Search with Score Threshold

```go
results, err := store.SimilaritySearch(ctx, "query", 10,
    vectorstores.WithScoreThreshold(0.8),
)
```

### Search with Metadata Filters

```go
results, err := store.SimilaritySearch(ctx, "query", 10,
    vectorstores.WithFilters(map[string]interface{}{
        "category": "technology",
        "source": "documentation",
    }),
)
```

### Search within Namespace

```go
results, err := store.SimilaritySearch(ctx, "query", 10,
    vectorstores.WithNameSpace("my_namespace"),
)
```

## Hybrid Search

Hybrid search combines vector similarity with keyword search using Reciprocal Rank Fusion (RRF):

```go
store, err := neo4j.New(ctx,
    // ... basic options ...
    neo4j.WithHybridSearch(true),
    neo4j.WithSearchType("hybrid"),
    neo4j.WithKeywordIndexName("keyword_index"),
)

// Performs both vector and keyword search, then combines results
results, err := store.SimilaritySearch(ctx, "programming languages", 10)
```

## Document Management

### Adding Documents with Metadata

```go
docs := []schema.Document{
    {
        PageContent: "Document content",
        Metadata: map[string]interface{}{
            "title": "Document Title",
            "author": "John Doe",
            "category": "technical",
            "tags": []string{"programming", "tutorial"},
        },
    },
}

ids, err := store.AddDocuments(ctx, docs)
```

### Adding Documents to Namespace

```go
ids, err := store.AddDocuments(ctx, docs,
    vectorstores.WithNameSpace("documentation"),
)
```

### Document Deduplication

```go
// Deduplicator function to prevent duplicate documents
deduplicator := func(ctx context.Context, doc schema.Document) bool {
    // Return true if document should be skipped (is duplicate)
    if title, exists := doc.Metadata["title"]; exists {
        return title == "Already Processed"
    }
    return false
}

ids, err := store.AddDocuments(ctx, docs,
    vectorstores.WithDeduplicater(deduplicator),
)
```

## Performance Considerations

### Vector Index Configuration

Neo4j vector indexes use HNSW (Hierarchical Navigable Small World) algorithm:

- **Dimensions**: Must match your embedding model (e.g., 1536 for OpenAI ada-002)
- **Similarity Function**: Choose `cosine` for normalized vectors, `euclidean` for raw vectors
- **Index Creation**: Can take time for large datasets

### Batch Operations

For large document collections, consider batch processing:

```go
batchSize := 1000
for i := 0; i < len(docs); i += batchSize {
    end := i + batchSize
    if end > len(docs) {
        end = len(docs)
    }
    
    batch := docs[i:end]
    _, err := store.AddDocuments(ctx, batch)
    if err != nil {
        log.Printf("Error processing batch %d: %v", i/batchSize, err)
    }
}
```

## Error Handling

The Neo4j vector store defines several specific error types:

```go
import "errors"

if err != nil {
    switch {
    case errors.Is(err, neo4j.ErrEmbedderNotSet):
        log.Fatal("Embedder must be provided")
    case errors.Is(err, neo4j.ErrInvalidDimensions):
        log.Fatal("Vector dimensions must be positive")
    case errors.Is(err, neo4j.ErrInvalidSimilarityFunc):
        log.Fatal("Similarity function must be 'cosine' or 'euclidean'")
    default:
        log.Fatalf("Unexpected error: %v", err)
    }
}
```

## Cypher Query Examples

The Neo4j vector store generates Cypher queries for vector operations. Here are some examples:

### Vector Index Creation

```cypher
CREATE VECTOR INDEX documents IF NOT EXISTS
FOR (n:Document) ON (n.embedding)
OPTIONS {
  indexConfig: {
    'vector.dimensions': 1536,
    'vector.similarity_function': 'cosine'
  }
}
```

### Vector Similarity Search

```cypher
CALL db.index.vector.queryNodes($indexName, $numDocuments, $queryVector)
YIELD node, score
WHERE node.metadata.category = $category
RETURN 
  node.text AS text,
  node.metadata AS metadata,
  score
ORDER BY score DESC
```

### Hybrid Search with RRF

```cypher
WITH $query AS query, $queryVector AS queryVector
CALL {
  WITH queryVector
  CALL db.index.vector.queryNodes($vectorIndex, $numDocs, queryVector)
  YIELD node, score
  RETURN node, score, "vector" AS searchType
}
UNION ALL
CALL {
  WITH query
  CALL db.index.fulltext.queryNodes($keywordIndex, query)
  YIELD node, score
  RETURN node, score, "keyword" AS searchType
}
WITH node, searchType, score,
  CASE searchType
    WHEN "vector" THEN 0
    WHEN "keyword" THEN 1
  END AS rankType
ORDER BY rankType, score DESC
WITH collect({node: node, score: score, searchType: searchType}) AS results
UNWIND range(0, size(results)-1) AS i
WITH results[i].node AS node,
  results[i].searchType AS searchType,
  results[i].score AS originalScore,
  (1.0 / ($rrf_k + i + 1)) AS rrfScore,
  i AS rank
RETURN 
  node.text AS text,
  node.metadata AS metadata,
  sum(rrfScore) AS combinedScore
ORDER BY combinedScore DESC
LIMIT $limit
```

## Troubleshooting

### Common Issues

1. **Vector Index Not Found**
   - Ensure `WithCreateIndex(true)` is set
   - Check that Neo4j version is 5.11+

2. **Dimension Mismatch**
   - Verify embedding dimensions match index configuration
   - Common dimensions: OpenAI ada-002 (1536), sentence-transformers (384, 768)

3. **Connection Issues**
   - Verify Neo4j is running and accessible
   - Check connection URL format: `bolt://host:port`
   - Ensure authentication credentials are correct

4. **APOC Plugin Missing**
   - Some operations require APOC plugin
   - Install APOC or use Neo4j AuraDB which includes it

### Debug Mode

Enable debug logging to see generated Cypher queries:

```go
// In your Neo4j configuration or logging setup
// This will help you understand what queries are being executed
```

## Integration with LangChain Go

The Neo4j vector store implements the `vectorstores.VectorStore` interface and can be used with any LangChain Go retrieval chains:

```go
// Create retriever from vector store
retriever := vectorstores.ToRetriever(store, 10)

// Use with chains
// chain := chains.NewRetrievalQA(llm, retriever)
```

## Contributing

Contributions to improve the Neo4j vector store are welcome! Please ensure:

1. Tests pass with a running Neo4j instance
2. Follow existing code style and patterns
3. Add tests for new features
4. Update documentation for new options