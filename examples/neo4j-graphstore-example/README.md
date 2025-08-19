# Neo4j GraphStore Example

This example demonstrates how to use the LangChain Go Neo4j GraphStore to store and query graph data.

## Prerequisites

1. **Neo4j Database**: You need a running Neo4j instance. You can:
   - Download and install [Neo4j Desktop](https://neo4j.com/download/)
   - Run Neo4j with Docker: `docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest`
   - Use [Neo4j Aura](https://neo4j.com/cloud/aura/) (cloud service)

2. **APOC Plugin**: The example uses APOC procedures for advanced graph operations. Most Neo4j installations include APOC by default.

## Configuration

The example can be configured through:

### Code Configuration
```go
store, err := neo4j.New(
    neo4j.WithURI("bolt://localhost:7687"),
    neo4j.WithAuth("neo4j", "password"),
    neo4j.WithDatabase("neo4j"),
    // ... other options
)
```

### Environment Variables
```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USERNAME="neo4j"
export NEO4J_PASSWORD="password"
export NEO4J_DATABASE="neo4j"
```

## Running the Example

1. Start your Neo4j database
2. Update the connection details in `main.go` or set environment variables
3. Run the example:

```bash
cd examples/neo4j-graphstore-example
go mod tidy
go run main.go
```

## Features Demonstrated

### Basic Operations
- **Connection**: Establishing connection with configuration options
- **Graph Import**: Adding nodes and relationships from structured documents
- **Schema Management**: Refreshing and querying graph schema
- **Querying**: Executing Cypher queries with parameters

### Advanced Features
- **Value Sanitization**: Removing large arrays and embedding-like data
- **Enhanced Schema**: Including property examples and value ranges
- **Base Entity Labels**: Optional performance optimization
- **Timeout Support**: Preventing long-running queries
- **APOC Integration**: Using APOC procedures for advanced operations

### Configuration Options

```go
// Performance and behavior options
neo4j.WithSanitize(true)              // Remove large lists/embeddings from results
neo4j.WithEnhancedSchema(true)        // Include example values in schema
neo4j.WithBaseEntityLabel(false)      // Use __Entity__ label for performance
neo4j.WithTimeout(30*time.Second)     // Query timeout

// Connection tuning
neo4j.WithMaxConnectionLifetime(1*time.Hour)
neo4j.WithMaxConnectionPoolSize(50)
neo4j.WithConnectionAcquisitionTimeout(30*time.Second)
```

## Expected Output

When you run the example successfully, you should see:

```
Importing graph document...
✓ Graph document imported successfully

Refreshing schema...
Graph Schema:
Node properties:
Person {name: STRING, age: INTEGER}
Company {name: STRING, industry: STRING}
Document {id: STRING, text: STRING, source: STRING, category: STRING}
Relationship properties:
WORKS_AT {since: STRING}
KNOWS {relationship: STRING}
MENTIONS {}
The relationships:
(:Person)-[:WORKS_AT]->(:Company)
(:Person)-[:KNOWS]->(:Person)
(:Document)-[:MENTIONS]->(:Person)
(:Document)-[:MENTIONS]->(:Company)

Structured schema contains 3 node types and 3 relationship types

Executing sample queries...
People in the graph:
  - Alice (age: 30)
  - Bob (age: 25)

Relationships:
  Alice -[WORKS_AT]-> ACME Corp
  Bob -[WORKS_AT]-> XYZ Inc
  Alice -[KNOWS]-> Bob

Technology company employees:
  Alice works at ACME Corp

✓ Example completed successfully
```

## Troubleshooting

### Connection Issues
- Verify Neo4j is running on the specified port
- Check username/password credentials
- Ensure the database name exists

### APOC Issues
- Install APOC plugin if not available
- Verify APOC procedures are enabled in Neo4j configuration
- Check Neo4j logs for APOC-related errors

### Performance Issues
- Enable `WithBaseEntityLabel(true)` for large datasets
- Adjust connection pool settings
- Set appropriate query timeouts

## Next Steps

- Explore the [Neo4j Cypher documentation](https://neo4j.com/docs/cypher-manual/current/)
- Learn about [APOC procedures](https://neo4j.com/labs/apoc/)
- Check out the [LangChain Go documentation](https://pkg.go.dev/github.com/tmc/langchaingo)