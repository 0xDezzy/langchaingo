package neo4j

import (
	"github.com/tmc/langchaingo/embeddings"
)

// Option is a function type for configuring a Neo4j vector store.
type Option func(*options)

// options contains the configuration for the Neo4j vector store.
type options struct {
	connectionURL    string
	username         string
	password         string
	database         string
	embedder         embeddings.Embedder
	indexName        string
	nodeLabel        string
	embeddingProp    string
	textProp         string
	metadataProp     string
	idProp           string
	dimensions       int
	similarityFunc   string
	createIndex      bool
	indexConfig      map[string]interface{}
	preDeleteIndex   bool
	searchType       string
	hybridSearch     bool
	keywordIndexName string
	rrf              map[string]interface{}
}

// defaultOptions returns the default options for Neo4j vector store.
func defaultOptions() *options {
	return &options{
		connectionURL:    "bolt://localhost:7687",
		username:         "neo4j",
		password:         "password",
		database:         "neo4j",
		indexName:        "vector-index",
		nodeLabel:        "Document",
		embeddingProp:    "embedding",
		textProp:         "text",
		metadataProp:     "metadata",
		idProp:           "id",
		dimensions:       1536, // OpenAI default
		similarityFunc:   "cosine",
		createIndex:      true,
		indexConfig:      make(map[string]interface{}),
		preDeleteIndex:   false,
		searchType:       "vector",
		hybridSearch:     false,
		keywordIndexName: "keyword-index",
		rrf:              map[string]interface{}{"k": 60},
	}
}

// WithConnectionURL sets the Neo4j connection URL.
func WithConnectionURL(url string) Option {
	return func(o *options) {
		o.connectionURL = url
	}
}

// WithCredentials sets the Neo4j authentication credentials.
func WithCredentials(username, password string) Option {
	return func(o *options) {
		o.username = username
		o.password = password
	}
}

// WithDatabase sets the Neo4j database name.
func WithDatabase(database string) Option {
	return func(o *options) {
		o.database = database
	}
}

// WithEmbedder sets the embeddings provider.
func WithEmbedder(embedder embeddings.Embedder) Option {
	return func(o *options) {
		o.embedder = embedder
	}
}

// WithIndexName sets the name of the vector index.
func WithIndexName(name string) Option {
	return func(o *options) {
		o.indexName = name
	}
}

// WithNodeLabel sets the Neo4j node label for documents.
func WithNodeLabel(label string) Option {
	return func(o *options) {
		o.nodeLabel = label
	}
}

// WithEmbeddingProperty sets the property name for storing embeddings.
func WithEmbeddingProperty(prop string) Option {
	return func(o *options) {
		o.embeddingProp = prop
	}
}

// WithTextProperty sets the property name for storing document text.
func WithTextProperty(prop string) Option {
	return func(o *options) {
		o.textProp = prop
	}
}

// WithMetadataProperty sets the property name for storing document metadata.
func WithMetadataProperty(prop string) Option {
	return func(o *options) {
		o.metadataProp = prop
	}
}

// WithIDProperty sets the property name for storing document IDs.
func WithIDProperty(prop string) Option {
	return func(o *options) {
		o.idProp = prop
	}
}

// WithDimensions sets the vector dimensions for the index.
func WithDimensions(dims int) Option {
	return func(o *options) {
		o.dimensions = dims
	}
}

// WithSimilarityFunction sets the similarity function (cosine or euclidean).
func WithSimilarityFunction(fn string) Option {
	return func(o *options) {
		o.similarityFunc = fn
	}
}

// WithCreateIndex controls whether to automatically create the vector index.
func WithCreateIndex(create bool) Option {
	return func(o *options) {
		o.createIndex = create
	}
}

// WithIndexConfig sets additional index configuration options.
func WithIndexConfig(config map[string]interface{}) Option {
	return func(o *options) {
		o.indexConfig = config
	}
}

// WithPreDeleteIndex controls whether to delete existing index before creating new one.
func WithPreDeleteIndex(preDelete bool) Option {
	return func(o *options) {
		o.preDeleteIndex = preDelete
	}
}

// WithSearchType sets the search type (vector, hybrid).
func WithSearchType(searchType string) Option {
	return func(o *options) {
		o.searchType = searchType
	}
}

// WithHybridSearch enables hybrid search combining vector and keyword search.
func WithHybridSearch(enable bool) Option {
	return func(o *options) {
		o.hybridSearch = enable
	}
}

// WithKeywordIndexName sets the name of the keyword index for hybrid search.
func WithKeywordIndexName(name string) Option {
	return func(o *options) {
		o.keywordIndexName = name
	}
}

// WithRRF sets the reciprocal rank fusion parameters for hybrid search.
func WithRRF(rrf map[string]interface{}) Option {
	return func(o *options) {
		o.rrf = rrf
	}
}
