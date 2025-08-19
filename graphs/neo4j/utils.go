package neo4j

import (
	"crypto/md5"
	"fmt"
	"strings"

	"github.com/tmc/langchaingo/schema"
)

// Helper functions

// cleanString removes backticks and cleans strings for Neo4j usage
func cleanString(text string) string {
	return strings.ReplaceAll(text, "`", "")
}

// generateDocumentID generates an ID for a document
func generateDocumentID(doc schema.Document) string {
	if id, exists := doc.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok && idStr != "" {
			return idStr
		}
	}
	// Generate MD5 hash of page content as fallback
	return fmt.Sprintf("%x", md5Hash([]byte(doc.PageContent)))
}

// md5Hash generates MD5 hash of byte slice
func md5Hash(data []byte) [16]byte {
	return md5.Sum(data)
}

// isAPOCError checks if an error is due to missing APOC procedures
func isAPOCError(err error) bool {
	if err == nil {
		return false
	}
	errorStr := err.Error()
	return strings.Contains(errorStr, "Neo.ClientError.Procedure.ProcedureNotFound") ||
		strings.Contains(errorStr, "apoc.meta.data") ||
		strings.Contains(errorStr, "apoc.merge.node") ||
		strings.Contains(errorStr, "apoc.merge.relationship")
}

// wrapAPOCError wraps APOC-related errors with helpful guidance
func wrapAPOCError(err error) error {
	if !isAPOCError(err) {
		return err
	}

	return fmt.Errorf("%w: %v\n\nAPOC procedures are not available. Please ensure:\n"+
		"1. APOC plugin is installed in your Neo4j instance\n"+
		"2. APOC procedures are allowed in Neo4j configuration\n"+
		"3. The procedures 'apoc.meta.data', 'apoc.merge.node', and 'apoc.merge.relationship' are accessible\n"+
		"Visit https://neo4j.com/labs/apoc/ for installation instructions",
		ErrAPOCNotAvailable, err)
}

// valueSanitize sanitizes input by removing embedding-like values and oversized lists.
// This prevents context pollution and improves LLM performance by filtering out
// irrelevant large data structures. Based on the Python implementation.
func valueSanitize(d interface{}) interface{} {
	switch v := d.(type) {
	case map[string]interface{}:
		newDict := make(map[string]interface{})
		for key, value := range v {
			sanitized := valueSanitize(value)
			if sanitized != nil {
				newDict[key] = sanitized
			}
		}
		return newDict

	case []interface{}:
		if len(v) >= LIST_LIMIT {
			return nil // Skip oversized lists (likely embeddings)
		}
		var newList []interface{}
		for _, item := range v {
			sanitized := valueSanitize(item)
			if sanitized != nil {
				newList = append(newList, sanitized)
			}
		}
		return newList

	default:
		return v
	}
}

// cleanStringValues cleans string values for schema display
func cleanStringValues(text string) string {
	// Replace newlines and carriage returns with spaces
	cleaned := strings.ReplaceAll(text, "\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\r", " ")
	return cleaned
}
