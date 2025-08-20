# KuzuDB GraphStore Example

This example demonstrates how to use KuzuDB as a graph store with LangChain Go. KuzuDB is a high-performance embedded graph database that supports Cypher queries and provides excellent performance for graph operations.

## Features Demonstrated

- **Connection Management**: Both in-memory and file-based database configurations
- **Graph Document Import**: Adding nodes and relationships with properties
- **Schema Operations**: Schema introspection and management
- **Query Execution**: Basic and parameterized Cypher queries
- **Transaction Management**: ACID transactions for batch operations
- **Type Conversion**: Proper handling of Go types to KuzuDB types
- **Performance Features**: Prepared statements, batch processing, and query optimization

## Prerequisites

KuzuDB requires CGO to be enabled. Make sure you have:

1. Go 1.23.8+ installed
2. KuzuDB C++ library installed on your system
3. CGO enabled (`CGO_ENABLED=1`)

## Installation

Install KuzuDB following the instructions at: https://kuzudb.com/docs/installation

For most systems:
```bash
# Linux/macOS
curl -L https://github.com/kuzudb/kuzu/releases/latest/download/kuzu-linux-x86_64.tar.gz | tar -xz

# Or follow system-specific installation instructions
```

## Running the Example

```bash
# Build the example
CGO_ENABLED=1 go build -o kuzu-example ./examples/kuzu-graphstore-example

# Run the example
./kuzu-example
```

## Configuration Options

The example demonstrates various KuzuDB configuration options:

### Database Configuration
- `WithInMemory(true)` - Use in-memory database (great for testing)
- `WithDatabasePath("./path")` - Use file-based database (persistent storage)
- `WithAllowDangerousRequests(true)` - Required to enable arbitrary Cypher queries

### Performance Options
- `WithMaxNumThreads(n)` - Set maximum threads for query execution
- `WithTimeout(duration)` - Set query timeout
- `WithBufferPoolSize(bytes)` - Configure memory buffer pool size

### Logging Options
- `WithEnableLogging(true)` - Enable logging
- `WithLogLevel("info")` - Set log level (trace, debug, info, warn, error)

## Example Scenarios

### 1. In-Memory Database
Perfect for testing, caching, and temporary graph operations:
```go
store, err := kuzu.NewKuzu(
    kuzu.WithInMemory(true),
    kuzu.WithAllowDangerousRequests(true),
    kuzu.WithMaxNumThreads(2),
)
```

### 2. File-Based Database
For persistent storage and production use:
```go
store, err := kuzu.NewKuzu(
    kuzu.WithDatabasePath("./my_graph_db"),
    kuzu.WithAllowDangerousRequests(true),
    kuzu.WithTimeout(30*time.Second),
    kuzu.WithMaxNumThreads(4),
)
```

### 3. Transaction Support
For ACID compliance in batch operations:
```go
err := store.RunInTransaction(ctx, func(tx *kuzu.Transaction) error {
    return store.AddGraphDocumentsWithTransaction(tx, documents)
})
```

## Graph Data Model

The example creates a rich graph model with:

### Node Types
- **Person**: Employees with properties (name, age, department)
- **Company**: Organizations with properties (name, industry, employees, value)
- **Project**: Work projects with properties (name, status, budget)
- **Team**: Working groups with properties (name, size, specialization)

### Relationship Types
- **WORKS_AT**: Person → Company (since, position)
- **KNOWS**: Person → Person (since, strength)
- **WORKED_ON**: Person → Project (role, hours)
- **COLLEAGUE**: Person → Person (team, since)
- **REPORTS_TO**: Person → Person (since)
- **ACQUIRED**: Company → Company (amount, date, currency)
- **OWNS**: Company → Project (since)
- **MANAGES**: Team → Project (responsibility)
- **BELONGS_TO**: Team → Company (retained)

## Query Examples

### Find All People
```cypher
MATCH (p:Person) 
RETURN p.name as name, p.age as age
```

### Find Relationships
```cypher
MATCH (p1:Person)-[r:KNOWS]->(p2:Person) 
RETURN p1.name as person1, p2.name as person2, r.since as since
```

### Find Companies and Their Values
```cypher
MATCH (c:Company) 
RETURN c.name as company, c.value as value, c.founded as founded
```

### Complex Acquisition Query
```cypher
MATCH (acquirer:Company)-[acq:ACQUIRED]->(acquired:Company)
RETURN acquirer.name as acquirer, acquired.name as acquired, 
       acq.amount as amount, acq.date as date
```

## Advanced Features

### Type Conversion
KuzuDB automatically handles conversion between Go types and database types:
- `string` → `STRING`
- `int`, `int64` → `INT64`
- `float64` → `DOUBLE`
- `bool` → `BOOL`
- `time.Time` → `DATE` or `TIMESTAMP`
- `[]interface{}` → `LIST`
- `map[string]interface{}` → `STRUCT`

### Prepared Statements
For better performance with repeated queries:
```go
stmt, err := store.PrepareQuery("MATCH (c:Company {name: $name}) RETURN c.value")
result, err := store.ExecutePreparedQuery(ctx, stmt, params)
```

### Schema Introspection
Discover the database structure programmatically:
```go
err := store.RefreshSchema(ctx)
schema := store.GetSchema()                    // Human-readable format
structured := store.GetStructuredSchema()     // Programmatic access
tables, err := store.GetTableList(ctx)        // List all tables
```

### Batch Processing
Efficient import of large datasets:
```go
// Automatic batching based on document size
err := store.AddGraphDocuments(ctx, manyDocuments)

// Manual transaction control
err := store.RunInTransaction(ctx, func(tx *kuzu.Transaction) error {
    return store.AddGraphDocumentsWithTransaction(tx, documents)
})
```

## Performance Tips

1. **Use In-Memory for Testing**: In-memory databases are much faster for development and testing
2. **Configure Buffer Pool**: Increase buffer pool size for large datasets
3. **Use Transactions**: Batch operations in transactions for better performance and consistency
4. **Prepared Statements**: Use prepared statements for repeated queries
5. **Optimize Thread Count**: Set thread count based on your CPU cores
6. **Index Properties**: KuzuDB automatically creates indexes on primary keys

## Error Handling

The example demonstrates proper error handling for:
- Connection failures
- Invalid queries
- Transaction rollbacks
- Type conversion errors
- Schema validation

## Troubleshooting

### Common Issues

1. **CGO Not Enabled**
   ```
   Error: CGO_ENABLED=0, but cgo is required
   Solution: Set CGO_ENABLED=1
   ```

2. **KuzuDB Library Not Found**
   ```
   Error: cannot find KuzuDB shared library
   Solution: Install KuzuDB system-wide or set LD_LIBRARY_PATH
   ```

3. **Dangerous Requests Disabled**
   ```
   Error: dangerous requests are disabled
   Solution: Add WithAllowDangerousRequests(true) option
   ```

### Debug Mode
Enable detailed logging for debugging:
```go
store, err := kuzu.NewKuzu(
    kuzu.WithInMemory(true),
    kuzu.WithAllowDangerousRequests(true),
    kuzu.WithEnableLogging(true),
    kuzu.WithLogLevel("debug"),
)
```

## Next Steps

- Explore the [KuzuDB documentation](https://kuzudb.com/docs) for advanced Cypher features
- Check out the [LangChain Go documentation](https://pkg.go.dev/github.com/tmc/langchaingo) for integration patterns
- Experiment with different graph models for your use case
- Benchmark performance with your specific data patterns

## License

This example is part of the LangChain Go project and follows the same license terms.