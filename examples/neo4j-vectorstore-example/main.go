package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/tmc/langchaingo/vectorstores/neo4j"
)

func main() {
	ctx := context.Background()

	// Get configuration from environment
	neo4jURL := getEnvOrDefault("NEO4J_URL", "bolt://localhost:7687")
	neo4jUser := getEnvOrDefault("NEO4J_USERNAME", "neo4j")
	neo4jPass := getEnvOrDefault("NEO4J_PASSWORD", "password")
	openaiKey := os.Getenv("OPENAI_API_KEY")

	if openaiKey == "" {
		log.Fatal("OPENAI_API_KEY environment variable is required")
	}

	// Create OpenAI LLM for embeddings
	llm, err := openai.New()
	if err != nil {
		log.Fatalf("Failed to create OpenAI client: %v", err)
	}

	// Create embeddings using OpenAI
	embedder, err := embeddings.NewEmbedder(llm)
	if err != nil {
		log.Fatalf("Failed to create embedder: %v", err)
	}

	// Create Neo4j vector store
	store, err := neo4j.New(ctx,
		neo4j.WithConnectionURL(neo4jURL),
		neo4j.WithCredentials(neo4jUser, neo4jPass),
		neo4j.WithEmbedder(embedder),
		neo4j.WithIndexName("document_embeddings"),
		neo4j.WithNodeLabel("Document"),
		neo4j.WithDimensions(1536), // OpenAI ada-002 dimensions
		neo4j.WithSimilarityFunction("cosine"),
		neo4j.WithPreDeleteIndex(true), // Clean up existing index for demo
	)
	if err != nil {
		log.Fatalf("Failed to create Neo4j store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("Error closing store: %v", err)
		}
	}()

	fmt.Println("🚀 Neo4j Vector Store Example")
	fmt.Println("=============================")

	// Sample documents to add to the vector store
	documents := []schema.Document{
		{
			PageContent: "The quick brown fox jumps over the lazy dog. This is a classic pangram used in typing practice.",
			Metadata: map[string]interface{}{
				"source":   "typing_practice",
				"category": "text_samples",
				"length":   "short",
			},
		},
		{
			PageContent: "Python is a high-level programming language known for its simplicity and readability. It's widely used in web development, data science, and artificial intelligence.",
			Metadata: map[string]interface{}{
				"source":   "programming_guide",
				"category": "technology",
				"language": "python",
			},
		},
		{
			PageContent: "Machine learning is a subset of artificial intelligence that enables computers to learn and make decisions from data without explicit programming.",
			Metadata: map[string]interface{}{
				"source":   "ai_encyclopedia",
				"category": "technology",
				"field":    "machine_learning",
			},
		},
		{
			PageContent: "Neo4j is a graph database management system that stores data in nodes and relationships, making it perfect for connected data scenarios.",
			Metadata: map[string]interface{}{
				"source":   "database_guide",
				"category": "technology",
				"type":     "graph_database",
			},
		},
		{
			PageContent: "Vector databases are specialized databases designed to store and query high-dimensional vectors, enabling efficient similarity search for AI applications.",
			Metadata: map[string]interface{}{
				"source":   "database_guide",
				"category": "technology",
				"type":     "vector_database",
			},
		},
	}

	fmt.Printf("📝 Adding %d documents to Neo4j vector store...\n", len(documents))

	// Add documents to the store
	ids, err := store.AddDocuments(ctx, documents)
	if err != nil {
		log.Fatalf("Failed to add documents: %v", err)
	}

	fmt.Printf("✅ Successfully added %d documents with IDs: %v\n\n", len(ids), ids)

	// Perform similarity searches
	queries := []string{
		"programming languages",
		"artificial intelligence",
		"database systems",
		"typing and text",
	}

	for i, query := range queries {
		fmt.Printf("🔍 Search %d: '%s'\n", i+1, query)
		fmt.Println("----------------------------------------")

		// Basic similarity search
		results, err := store.SimilaritySearch(ctx, query, 3)
		if err != nil {
			log.Printf("Error performing search: %v", err)
			continue
		}

		for j, doc := range results {
			fmt.Printf("  %d. Score: %.4f\n", j+1, doc.Score)
			fmt.Printf("     Text: %s\n", truncateString(doc.PageContent, 80))
			if doc.Metadata != nil {
				if category, ok := doc.Metadata["category"]; ok {
					fmt.Printf("     Category: %s\n", category)
				}
			}
			fmt.Println()
		}
	}

	// Demonstrate filtered search
	fmt.Println("🎯 Filtered Search Example")
	fmt.Println("==========================")
	fmt.Println("Searching for 'computer' in 'technology' category only...")

	filteredResults, err := store.SimilaritySearch(
		ctx,
		"computer",
		5,
		vectorstores.WithFilters(map[string]interface{}{"category": "technology"}),
	)
	if err != nil {
		log.Printf("Error performing filtered search: %v", err)
	} else {
		fmt.Printf("Found %d results:\n", len(filteredResults))
		for i, doc := range filteredResults {
			fmt.Printf("  %d. Score: %.4f - %s\n", i+1, doc.Score, truncateString(doc.PageContent, 60))
		}
	}

	fmt.Println("\n🎉 Demo completed successfully!")
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// truncateString truncates a string to specified length with ellipsis
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
