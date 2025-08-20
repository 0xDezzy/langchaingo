package neo4j

import (
	"context"
	"fmt"
	"strings"
)

// RefreshSchema refreshes the schema information from the Neo4j database
func (n *Neo4j) RefreshSchema(ctx context.Context) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	n.schemaMux.Lock()
	defer n.schemaMux.Unlock()

	// Query node properties
	nodePropsQuery := `
		CALL apoc.meta.data()
		YIELD label, other, elementType, type, property
		WHERE NOT type = "RELATIONSHIP" AND elementType = "node" 
		  AND NOT label IN $EXCLUDED_LABELS
		WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
		RETURN {labels: nodeLabels, properties: properties} AS output
	`

	// Query relationship properties
	relPropsQuery := `
		CALL apoc.meta.data()
		YIELD label, other, elementType, type, property
		WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
		      AND NOT label in $EXCLUDED_LABELS
		WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
		RETURN {type: nodeLabels, properties: properties} AS output
	`

	// Query relationships
	relQuery := `
		CALL apoc.meta.data()
		YIELD label, other, elementType, type, property
		WHERE type = "RELATIONSHIP" AND elementType = "node"
		UNWIND other AS other_node
		WITH * WHERE NOT label IN $EXCLUDED_LABELS
		    AND NOT other_node IN $EXCLUDED_LABELS
		RETURN {start: label, type: property, end: toString(other_node)} AS output
	`

	excludedLabels := []string{"_Bloom_Perspective_", "_Bloom_Scene_", "__Entity__"}
	excludedRels := []string{"_Bloom_HAS_SCENE_"}

	// Execute queries
	nodeResult, err := n.Query(ctx, nodePropsQuery, map[string]interface{}{
		"EXCLUDED_LABELS": excludedLabels,
	})
	if err != nil {
		if isAPOCError(err) {
			return wrapAPOCError(err)
		}
		return fmt.Errorf("failed to query node properties: %w", err)
	}

	relPropsResult, err := n.Query(ctx, relPropsQuery, map[string]interface{}{
		"EXCLUDED_LABELS": excludedRels,
	})
	if err != nil {
		return fmt.Errorf("failed to query relationship properties: %w", err)
	}

	relsResult, err := n.Query(ctx, relQuery, map[string]interface{}{
		"EXCLUDED_LABELS": excludedLabels,
	})
	if err != nil {
		return fmt.Errorf("failed to query relationships: %w", err)
	}

	// Build structured schema
	structuredSchema := make(map[string]interface{})

	// Process node properties
	nodeProps := make(map[string]interface{})
	if records, ok := nodeResult["records"].([]map[string]interface{}); ok {
		for _, record := range records {
			if output, exists := record["output"].(map[string]interface{}); exists {
				if labels, hasLabels := output["labels"].(string); hasLabels {
					if properties, hasProps := output["properties"]; hasProps {
						nodeProps[labels] = properties
					}
				}
			}
		}
	}

	// Process relationship properties
	relProps := make(map[string]interface{})
	if records, ok := relPropsResult["records"].([]map[string]interface{}); ok {
		for _, record := range records {
			if output, exists := record["output"].(map[string]interface{}); exists {
				if relType, hasType := output["type"].(string); hasType {
					if properties, hasProps := output["properties"]; hasProps {
						relProps[relType] = properties
					}
				}
			}
		}
	}

	// Process relationships
	var relationships []map[string]interface{}
	if records, ok := relsResult["records"].([]map[string]interface{}); ok {
		for _, record := range records {
			if output, exists := record["output"].(map[string]interface{}); exists {
				relationships = append(relationships, output)
			}
		}
	}

	structuredSchema["node_props"] = nodeProps
	structuredSchema["rel_props"] = relProps
	structuredSchema["relationships"] = relationships

	// Get constraints & indexes metadata
	metadata := make(map[string]interface{})

	// Try to get constraints
	constraintResult, err := n.Query(ctx, "SHOW CONSTRAINTS", nil)
	if err == nil {
		if records, ok := constraintResult["records"].([]map[string]interface{}); ok {
			metadata["constraint"] = records
		}
	} else {
		// Fallback: user might not have access to schema information
		metadata["constraint"] = []map[string]interface{}{}
	}

	// Try to get indexes
	indexQuery := "CALL apoc.schema.nodes() YIELD label, properties, type, size, valuesSelectivity " +
		"WHERE type = 'RANGE' RETURN *, size * valuesSelectivity as distinctValues"
	indexResult, err := n.Query(ctx, indexQuery, nil)
	if err == nil {
		if records, ok := indexResult["records"].([]map[string]interface{}); ok {
			metadata["index"] = records
		}
	} else {
		// Fallback: APOC might not be available or user lacks permissions
		metadata["index"] = []map[string]interface{}{}
	}

	structuredSchema["metadata"] = metadata
	n.structuredSchema = structuredSchema

	// Format schema as string
	n.schemaCache = n.formatSchema(structuredSchema)

	return nil
}

// GetSchema returns the current schema as a string representation
func (n *Neo4j) GetSchema() string {
	n.schemaMux.RLock()
	defer n.schemaMux.RUnlock()
	return n.schemaCache
}

// formatSchema formats the structured schema into a human-readable string
func (n *Neo4j) formatSchema(schema map[string]interface{}) string {
	var parts []string

	// Format node properties with enhanced details if enabled
	parts = append(parts, "Node properties:")
	if nodeProps, ok := schema["node_props"].(map[string]interface{}); ok {
		for label, props := range nodeProps {
			if propsList, ok := props.([]interface{}); ok {
				if n.enhancedSchema {
					parts = append(parts, fmt.Sprintf("- **%s**", label))
					for _, prop := range propsList {
						if propMap, ok := prop.(map[string]interface{}); ok {
							formatted := n.formatEnhancedProperty(propMap)
							if formatted != "" {
								parts = append(parts, fmt.Sprintf("  - %s", formatted))
							}
						}
					}
				} else {
					var propStrs []string
					for _, prop := range propsList {
						if propMap, ok := prop.(map[string]interface{}); ok {
							if name, hasName := propMap["property"].(string); hasName {
								if propType, hasType := propMap["type"].(string); hasType {
									propStrs = append(propStrs, fmt.Sprintf("%s: %s", name, propType))
								}
							}
						}
					}
					if len(propStrs) > 0 {
						parts = append(parts, fmt.Sprintf("%s {%s}", label, strings.Join(propStrs, ", ")))
					}
				}
			}
		}
	}

	// Format relationship properties
	parts = append(parts, "Relationship properties:")
	if relProps, ok := schema["rel_props"].(map[string]interface{}); ok {
		for relType, props := range relProps {
			if propsList, ok := props.([]interface{}); ok {
				if n.enhancedSchema {
					parts = append(parts, fmt.Sprintf("- **%s**", relType))
					for _, prop := range propsList {
						if propMap, ok := prop.(map[string]interface{}); ok {
							formatted := n.formatEnhancedProperty(propMap)
							if formatted != "" {
								parts = append(parts, fmt.Sprintf("  - %s", formatted))
							}
						}
					}
				} else {
					var propStrs []string
					for _, prop := range propsList {
						if propMap, ok := prop.(map[string]interface{}); ok {
							if name, hasName := propMap["property"].(string); hasName {
								if propType, hasType := propMap["type"].(string); hasType {
									propStrs = append(propStrs, fmt.Sprintf("%s: %s", name, propType))
								}
							}
						}
					}
					if len(propStrs) > 0 {
						parts = append(parts, fmt.Sprintf("%s {%s}", relType, strings.Join(propStrs, ", ")))
					}
				}
			}
		}
	}

	// Format relationships
	parts = append(parts, "The relationships:")
	if relationships, ok := schema["relationships"].([]map[string]interface{}); ok {
		for _, rel := range relationships {
			if start, hasStart := rel["start"].(string); hasStart {
				if relType, hasType := rel["type"].(string); hasType {
					if end, hasEnd := rel["end"].(string); hasEnd {
						parts = append(parts, fmt.Sprintf("(:%s)-[:%s]->(:%s)", start, relType, end))
					}
				}
			}
		}
	}

	return strings.Join(parts, "\n")
}

// formatEnhancedProperty formats a property with enhanced details like examples, ranges, etc.
func (n *Neo4j) formatEnhancedProperty(propMap map[string]interface{}) string {
	name, hasName := propMap["property"].(string)
	propType, hasType := propMap["type"].(string)

	if !hasName || !hasType {
		return ""
	}

	var example string

	switch propType {
	case "STRING":
		if values, hasValues := propMap["values"].([]interface{}); hasValues && len(values) > 0 {
			if distinctCount, hasCount := propMap["distinct_count"]; hasCount {
				if count, ok := distinctCount.(int); ok && count > DISTINCT_VALUE_LIMIT {
					if firstVal, ok := values[0].(string); ok {
						example = fmt.Sprintf(`Example: "%s"`, cleanStringValues(firstVal))
					}
				} else {
					// Show all available options if under limit
					var cleanValues []string
					for _, val := range values {
						if strVal, ok := val.(string); ok {
							cleanValues = append(cleanValues, cleanStringValues(strVal))
						}
					}
					example = fmt.Sprintf("Available options: %v", cleanValues)
				}
			}
		}

	case "INTEGER", "FLOAT", "DATE", "DATE_TIME", "LOCAL_DATE_TIME":
		if min, hasMin := propMap["min"]; hasMin {
			if max, hasMax := propMap["max"]; hasMax {
				example = fmt.Sprintf("Min: %v, Max: %v", min, max)
			}
		} else if values, hasValues := propMap["values"].([]interface{}); hasValues && len(values) > 0 {
			example = fmt.Sprintf(`Example: "%v"`, values[0])
		}

	case "LIST":
		if minSize, hasMin := propMap["min_size"]; hasMin {
			if min, ok := minSize.(int); ok && min <= LIST_LIMIT {
				if maxSize, hasMax := propMap["max_size"]; hasMax {
					example = fmt.Sprintf("Min Size: %v, Max Size: %v", minSize, maxSize)
				}
			} else {
				// Skip oversized lists (likely embeddings)
				return ""
			}
		}
	}

	if example != "" {
		return fmt.Sprintf("`%s`: %s %s", name, propType, example)
	}
	return fmt.Sprintf("`%s`: %s", name, propType)
}

// enhancedSchemaCypher generates Cypher queries for enhanced schema information
func (n *Neo4j) enhancedSchemaCypher(labelOrType string, properties []interface{}, exhaustive bool, isRelationship bool) string {
	var matchClause string
	if isRelationship {
		matchClause = fmt.Sprintf("MATCH ()-[n:`%s`]->()", labelOrType)
	} else {
		matchClause = fmt.Sprintf("MATCH (n:`%s`)", labelOrType)
	}

	var withClauses []string
	outputDict := make(map[string]string)

	if !exhaustive {
		// Just sample 5 random nodes
		matchClause += " WITH n LIMIT 5"
	}

	for _, prop := range properties {
		if propMap, ok := prop.(map[string]interface{}); ok {
			propName, hasProp := propMap["property"].(string)
			propType, hasType := propMap["type"].(string)

			if !hasProp || !hasType {
				continue
			}

			switch propType {
			case "STRING":
				withClauses = append(withClauses,
					fmt.Sprintf("collect(distinct substring(toString(n.`%s`), 0, 50)) AS `%s_values`", propName, propName))
				if exhaustive {
					outputDict[propName] = fmt.Sprintf("{values:`%s_values`[..%d], distinct_count: size(`%s_values`)}",
						propName, DISTINCT_VALUE_LIMIT, propName)
				} else {
					outputDict[propName] = fmt.Sprintf("{values: `%s_values`}", propName)
				}

			case "INTEGER", "FLOAT", "DATE", "DATE_TIME", "LOCAL_DATE_TIME":
				if exhaustive {
					withClauses = append(withClauses, fmt.Sprintf("min(n.`%s`) AS `%s_min`", propName, propName))
					withClauses = append(withClauses, fmt.Sprintf("max(n.`%s`) AS `%s_max`", propName, propName))
					withClauses = append(withClauses, fmt.Sprintf("count(distinct n.`%s`) AS `%s_distinct`", propName, propName))
					outputDict[propName] = fmt.Sprintf("{min: toString(`%s_min`), max: toString(`%s_max`), distinct_count: `%s_distinct`}",
						propName, propName, propName)
				} else {
					withClauses = append(withClauses, fmt.Sprintf("collect(distinct toString(n.`%s`)) AS `%s_values`", propName, propName))
					outputDict[propName] = fmt.Sprintf("{values: `%s_values`}", propName)
				}

			case "LIST":
				withClauses = append(withClauses,
					fmt.Sprintf("min(size(n.`%s`)) AS `%s_size_min`, max(size(n.`%s`)) AS `%s_size_max`",
						propName, propName, propName, propName))
				outputDict[propName] = fmt.Sprintf("{min_size: `%s_size_min`, max_size: `%s_size_max`}", propName, propName)

			case "BOOLEAN", "POINT", "DURATION":
				// Skip these types
				continue
			}
		}
	}

	if len(withClauses) == 0 {
		return ""
	}

	withClause := "WITH " + strings.Join(withClauses, ",\n     ")

	var returnParts []string
	for prop, expr := range outputDict {
		returnParts = append(returnParts, fmt.Sprintf("`%s`: %s", prop, expr))
	}
	returnClause := "RETURN {" + strings.Join(returnParts, ", ") + "} AS output"

	return strings.Join([]string{matchClause, withClause, returnClause}, "\n")
}
