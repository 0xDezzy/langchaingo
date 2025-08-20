package neo4j

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
)

var (
	ErrEmbedderNotSet          = fmt.Errorf("embedder not set")
	ErrInvalidScoreThreshold   = fmt.Errorf("score threshold must be between 0 and 1")
	ErrInvalidDimensions       = fmt.Errorf("vector dimensions must be positive")
	ErrInvalidSimilarityFunc   = fmt.Errorf("similarity function must be 'cosine' or 'euclidean'")
	ErrIndexCreationFailed     = fmt.Errorf("failed to create vector index")
	ErrEmbeddingVectorMismatch = fmt.Errorf("number of embeddings does not match number of documents")
)

// Store is a Neo4j vector store implementation.
type Store struct {
	driver        neo4j.DriverWithContext
	opts          *options
	indexExists   bool
	keywordExists bool
}

var _ vectorstores.VectorStore = (*Store)(nil)

// New creates a new Neo4j vector store with the given options.
func New(ctx context.Context, opts ...Option) (*Store, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Validate options
	if err := validateOptions(options); err != nil {
		return nil, err
	}

	// Create Neo4j driver
	driver, err := neo4j.NewDriverWithContext(
		options.connectionURL,
		neo4j.BasicAuth(options.username, options.password, ""),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Neo4j driver: %w", err)
	}

	// Verify connectivity
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("failed to connect to Neo4j: %w", err)
	}

	store := &Store{
		driver: driver,
		opts:   options,
	}

	// Initialize indexes if needed
	if options.createIndex {
		if err := store.ensureVectorIndex(ctx); err != nil {
			store.Close()
			return nil, err
		}
		if options.hybridSearch {
			if err := store.ensureKeywordIndex(ctx); err != nil {
				store.Close()
				return nil, err
			}
		}
	}

	return store, nil
}

// Close closes the Neo4j driver connection.
func (s *Store) Close() error {
	return s.driver.Close(context.Background())
}

// AddDocuments adds documents to the Neo4j vector store.
func (s *Store) AddDocuments(
	ctx context.Context,
	docs []schema.Document,
	options ...vectorstores.Option,
) ([]string, error) {
	if len(docs) == 0 {
		return []string{}, nil
	}

	opts := s.getVectorStoreOptions(options...)

	// Get embedder
	embedder := s.opts.embedder
	if opts.Embedder != nil {
		embedder = opts.Embedder
	}
	if embedder == nil {
		return nil, ErrEmbedderNotSet
	}

	// Apply deduplication if specified
	docs = s.deduplicate(ctx, opts, docs)
	if len(docs) == 0 {
		return []string{}, nil
	}

	// Extract texts for embedding
	texts := make([]string, len(docs))
	for i, doc := range docs {
		texts[i] = doc.PageContent
	}

	// Generate embeddings
	embeddings, err := embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embeddings: %w", err)
	}

	if len(embeddings) != len(docs) {
		return nil, ErrEmbeddingVectorMismatch
	}

	// Prepare and execute Cypher query to store documents
	return s.insertDocuments(ctx, docs, embeddings, opts.NameSpace)
}

// SimilaritySearch performs a similarity search for documents.
func (s *Store) SimilaritySearch(
	ctx context.Context,
	query string,
	numDocuments int,
	options ...vectorstores.Option,
) ([]schema.Document, error) {
	opts := s.getVectorStoreOptions(options...)

	// Get embedder
	embedder := s.opts.embedder
	if opts.Embedder != nil {
		embedder = opts.Embedder
	}
	if embedder == nil {
		return nil, ErrEmbedderNotSet
	}

	// Generate query embedding
	queryEmbedding, err := embedder.EmbedQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to generate query embedding: %w", err)
	}

	// Validate score threshold
	if opts.ScoreThreshold < 0 || opts.ScoreThreshold > 1 {
		return nil, ErrInvalidScoreThreshold
	}

	// Perform search based on type
	switch s.opts.searchType {
	case "hybrid":
		if s.opts.hybridSearch {
			return s.hybridSearch(ctx, query, queryEmbedding, numDocuments, opts)
		}
		fallthrough
	default:
		return s.vectorSearch(ctx, queryEmbedding, numDocuments, opts)
	}
}

// insertDocuments inserts documents with their embeddings into Neo4j.
func (s *Store) insertDocuments(
	ctx context.Context,
	docs []schema.Document,
	embeddings [][]float32,
	namespace string,
) ([]string, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.opts.database})
	defer session.Close(ctx)

	// Generate IDs for documents
	ids := make([]string, len(docs))
	for i := range docs {
		ids[i] = uuid.New().String()
	}

	// Prepare Cypher query
	cypher := fmt.Sprintf(`
		UNWIND $docs AS doc
		CREATE (n:%s {
			%s: doc.id,
			%s: doc.text,
			%s: doc.metadata,
			%s: doc.embedding
		})
		RETURN n.%s AS id
	`, s.opts.nodeLabel, s.opts.idProp, s.opts.textProp,
		s.opts.metadataProp, s.opts.embeddingProp, s.opts.idProp)

	// Prepare parameters
	docParams := make([]map[string]interface{}, len(docs))
	for i, doc := range docs {
		metadata := doc.Metadata
		if metadata == nil {
			metadata = make(map[string]interface{})
		}

		// Add namespace to metadata if specified
		if namespace != "" {
			metadata["namespace"] = namespace
		}

		// Convert metadata to JSON string for Neo4j storage
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal metadata: %w", err)
		}

		// Convert float32 embeddings to Neo4j format
		embeddingValues := make([]float64, len(embeddings[i]))
		for j, val := range embeddings[i] {
			embeddingValues[j] = float64(val)
		}

		docParams[i] = map[string]interface{}{
			"id":        ids[i],
			"text":      doc.PageContent,
			"metadata":  string(metadataJSON),
			"embedding": embeddingValues,
		}
	}

	// Execute query
	result, err := session.Run(ctx, cypher, map[string]interface{}{"docs": docParams})
	if err != nil {
		return nil, fmt.Errorf("failed to insert documents: %w", err)
	}

	// Consume the result to ensure the query is executed
	_, err = result.Consume(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to consume insert result: %w", err)
	}

	return ids, nil
}

// vectorSearch performs a pure vector similarity search.
func (s *Store) vectorSearch(
	ctx context.Context,
	queryEmbedding []float32,
	numDocuments int,
	opts *vectorstores.Options,
) ([]schema.Document, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.opts.database})
	defer session.Close(ctx)

	// Convert float32 to float64 for Neo4j
	embedding := make([]float64, len(queryEmbedding))
	for i, val := range queryEmbedding {
		embedding[i] = float64(val)
	}

	// Build where clause for filters and namespace
	whereClause := s.buildWhereClause(opts)

	// Prepare Cypher query using Neo4j vector index
	cypher := fmt.Sprintf(`
		CALL db.index.vector.queryNodes($indexName, $numDocuments, $queryVector)
		YIELD node, score
		WHERE %s
		RETURN 
			node.%s AS text,
			node.%s AS metadata,
			score
		ORDER BY score DESC
	`, whereClause, s.opts.textProp, s.opts.metadataProp)

	params := map[string]interface{}{
		"indexName":    s.opts.indexName,
		"numDocuments": numDocuments,
		"queryVector":  embedding,
	}

	// Add namespace filter if specified
	if opts.NameSpace != "" {
		params["namespace"] = opts.NameSpace
	}

	// Add custom filters
	if opts.Filters != nil {
		if filters, ok := opts.Filters.(map[string]interface{}); ok {
			for k, v := range filters {
				params[k] = v
			}
		}
	}

	result, err := session.Run(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("failed to execute vector search: %w", err)
	}

	// Process results
	docs := make([]schema.Document, 0, numDocuments)
	for result.Next(ctx) {
		record := result.Record()

		textVal, _ := record.Get("text")
		metadataVal, _ := record.Get("metadata")
		scoreVal, _ := record.Get("score")

		doc := schema.Document{
			PageContent: textVal.(string),
			Score:       float32(scoreVal.(float64)),
		}

		// Parse metadata from JSON string
		if metadataStr, ok := metadataVal.(string); ok && metadataStr != "" {
			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err == nil {
				doc.Metadata = metadata
			}
		}

		// Apply score threshold filter
		if opts.ScoreThreshold > 0 && doc.Score < opts.ScoreThreshold {
			continue
		}

		docs = append(docs, doc)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error processing search results: %w", err)
	}

	return docs, nil
}

// hybridSearch performs a hybrid search combining vector and keyword search.
func (s *Store) hybridSearch(
	ctx context.Context,
	query string,
	queryEmbedding []float32,
	numDocuments int,
	opts *vectorstores.Options,
) ([]schema.Document, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.opts.database})
	defer session.Close(ctx)

	// Convert float32 to float64 for Neo4j
	embedding := make([]float64, len(queryEmbedding))
	for i, val := range queryEmbedding {
		embedding[i] = float64(val)
	}

	cypher, params := s.buildHybridSearchQuery(query, embedding, numDocuments, opts)

	result, err := session.Run(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("failed to execute hybrid search: %w", err)
	}

	return s.processHybridSearchResults(result, numDocuments)
}

// buildHybridSearchQuery constructs the Cypher query and parameters for hybrid search.
func (s *Store) buildHybridSearchQuery(
	query string,
	embedding []float64,
	numDocuments int,
	opts *vectorstores.Options,
) (string, map[string]interface{}) {
	whereClause := s.buildWhereClause(opts)
	rrf_k := 60 // Default RRF k value
	if k, exists := s.opts.rrf["k"]; exists {
		if kInt, ok := k.(int); ok {
			rrf_k = kInt
		}
	}

	// Hybrid search using Reciprocal Rank Fusion (RRF)
	cypher := fmt.Sprintf(`
		WITH $query AS query, $queryVector AS queryVector
		CALL {
			WITH queryVector
			CALL db.index.vector.queryNodes($vectorIndex, $numDocs, queryVector)
			YIELD node, score
			WHERE %s
			RETURN node, score, "vector" AS searchType
		}
		UNION ALL
		CALL {
			WITH query
			CALL db.index.fulltext.queryNodes($keywordIndex, query)
			YIELD node, score
			WHERE %s
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
			node.%s AS text,
			node.%s AS metadata,
			sum(rrfScore) AS combinedScore
		ORDER BY combinedScore DESC
		LIMIT $limit
	`, whereClause, whereClause, s.opts.textProp, s.opts.metadataProp)

	params := map[string]interface{}{
		"query":        query,
		"queryVector":  embedding,
		"vectorIndex":  s.opts.indexName,
		"keywordIndex": s.opts.keywordIndexName,
		"numDocs":      numDocuments * 2, // Get more candidates for RRF
		"rrf_k":        rrf_k,
		"limit":        numDocuments,
	}

	// Add namespace and filters
	if opts.NameSpace != "" {
		params["namespace"] = opts.NameSpace
	}
	if opts.Filters != nil {
		if filters, ok := opts.Filters.(map[string]interface{}); ok {
			for k, v := range filters {
				params[k] = v
			}
		}
	}

	return cypher, params
}

// processHybridSearchResults processes the results from hybrid search.
func (s *Store) processHybridSearchResults(result neo4j.ResultWithContext, numDocuments int) ([]schema.Document, error) {
	docs := make([]schema.Document, 0, numDocuments)
	for result.Next(context.Background()) {
		record := result.Record()

		textVal, _ := record.Get("text")
		metadataVal, _ := record.Get("metadata")
		scoreVal, _ := record.Get("combinedScore")

		doc := schema.Document{
			PageContent: textVal.(string),
			Score:       float32(scoreVal.(float64)),
		}

		// Parse metadata from JSON string
		if metadataStr, ok := metadataVal.(string); ok && metadataStr != "" {
			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err == nil {
				doc.Metadata = metadata
			}
		}

		docs = append(docs, doc)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error processing hybrid search results: %w", err)
	}

	return docs, nil
}

// ensureVectorIndex creates the vector index if it doesn't exist.
func (s *Store) ensureVectorIndex(ctx context.Context) error {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.opts.database})
	defer session.Close(ctx)

	// Check if index exists
	checkQuery := "SHOW INDEXES YIELD name WHERE name = $indexName"
	result, err := session.Run(ctx, checkQuery, map[string]interface{}{"indexName": s.opts.indexName})
	if err != nil {
		return fmt.Errorf("failed to check index existence: %w", err)
	}

	hasIndex := result.Next(ctx)
	if err := result.Err(); err != nil {
		return fmt.Errorf("error checking index: %w", err)
	}

	// Delete existing index if pre-delete is enabled
	if hasIndex && s.opts.preDeleteIndex {
		dropQuery := fmt.Sprintf("DROP INDEX `%s` IF EXISTS", s.opts.indexName)
		if _, err := session.Run(ctx, dropQuery, nil); err != nil {
			return fmt.Errorf("failed to drop existing index: %w", err)
		}
		hasIndex = false
	}

	// Create index if it doesn't exist
	if !hasIndex {
		createQuery := fmt.Sprintf(`
			CREATE VECTOR INDEX `+"`%s`"+` IF NOT EXISTS
			FOR (n:%s) ON (n.%s)
			OPTIONS {
				indexConfig: {
					`+"`vector.dimensions`"+`: %d,
					`+"`vector.similarity_function`"+`: "%s"
				}
			}
		`, s.opts.indexName, s.opts.nodeLabel, s.opts.embeddingProp,
			s.opts.dimensions, s.opts.similarityFunc)

		if _, err := session.Run(ctx, createQuery, nil); err != nil {
			return fmt.Errorf("failed to create vector index: %w", err)
		}
	}

	s.indexExists = true
	return nil
}

// ensureKeywordIndex creates the fulltext index for hybrid search.
func (s *Store) ensureKeywordIndex(ctx context.Context) error {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.opts.database})
	defer session.Close(ctx)

	// Check if keyword index exists
	checkQuery := "SHOW INDEXES YIELD name WHERE name = $indexName"
	result, err := session.Run(ctx, checkQuery, map[string]interface{}{"indexName": s.opts.keywordIndexName})
	if err != nil {
		return fmt.Errorf("failed to check keyword index existence: %w", err)
	}

	hasIndex := result.Next(ctx)
	if err := result.Err(); err != nil {
		return fmt.Errorf("error checking keyword index: %w", err)
	}

	// Create keyword index if it doesn't exist
	if !hasIndex {
		createQuery := fmt.Sprintf(`
			CREATE FULLTEXT INDEX `+"`%s`"+` IF NOT EXISTS
			FOR (n:%s) ON EACH [n.%s]
		`, s.opts.keywordIndexName, s.opts.nodeLabel, s.opts.textProp)

		if _, err := session.Run(ctx, createQuery, nil); err != nil {
			return fmt.Errorf("failed to create keyword index: %w", err)
		}
	}

	s.keywordExists = true
	return nil
}

// buildWhereClause builds the WHERE clause for filtering.
func (s *Store) buildWhereClause(opts *vectorstores.Options) string {
	conditions := []string{}

	// Note: Advanced filtering with JSON metadata requires APOC or custom handling
	// For now, we'll disable complex filtering to ensure basic functionality works
	// TODO: Implement proper JSON metadata filtering

	if len(conditions) == 0 {
		return "true"
	}

	return strings.Join(conditions, " AND ")
}

// getVectorStoreOptions extracts vector store options from the provided options.
func (s *Store) getVectorStoreOptions(options ...vectorstores.Option) *vectorstores.Options {
	opts := &vectorstores.Options{}
	for _, opt := range options {
		opt(opts)
	}
	return opts
}

// deduplicate removes duplicate documents if a deduplicator is provided.
func (s *Store) deduplicate(
	ctx context.Context,
	opts *vectorstores.Options,
	docs []schema.Document,
) []schema.Document {
	if opts.Deduplicater == nil {
		return docs
	}

	filtered := make([]schema.Document, 0, len(docs))
	for _, doc := range docs {
		if !opts.Deduplicater(ctx, doc) {
			filtered = append(filtered, doc)
		}
	}
	return filtered
}

// validateOptions validates the provided options.
func validateOptions(opts *options) error {
	if opts.dimensions <= 0 {
		return ErrInvalidDimensions
	}

	if opts.similarityFunc != "cosine" && opts.similarityFunc != "euclidean" {
		return ErrInvalidSimilarityFunc
	}

	return nil
}
