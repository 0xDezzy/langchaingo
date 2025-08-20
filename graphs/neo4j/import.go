package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/tmc/langchaingo/graphs"
)

// AddGraphDocument adds graph documents to the Neo4j store
func (n *Neo4j) AddGraphDocument(ctx context.Context, docs []graphs.GraphDocument, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	// Create batches for efficient processing
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	for i := 0; i < len(docs); i += batchSize {
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		batch := docs[i:end]
		if err := n.processBatch(ctx, batch, opts); err != nil {
			return err
		}
	}

	return nil
}

// processBatch processes a batch of graph documents
func (n *Neo4j) processBatch(ctx context.Context, docs []graphs.GraphDocument, opts *graphs.Options) error {
	// Import nodes first
	for _, doc := range docs {
		if err := n.importNodes(ctx, doc, opts); err != nil {
			return err
		}
	}

	// Then import relationships
	for _, doc := range docs {
		if err := n.importRelationships(ctx, doc, opts); err != nil {
			return err
		}
	}

	return nil
}

// importNodes imports nodes from a graph document
func (n *Neo4j) importNodes(ctx context.Context, doc graphs.GraphDocument, opts *graphs.Options) error {
	if len(doc.Nodes) == 0 {
		return nil
	}

	// Ensure base entity constraint if needed
	if err := n.ensureBaseEntityConstraint(ctx); err != nil {
		return fmt.Errorf("failed to ensure base entity constraint: %w", err)
	}

	// Generate query using the appropriate method
	query := n.getNodeImportQuery(opts.IncludeSource)

	// Prepare node data
	var nodeData []map[string]interface{}
	for _, node := range doc.Nodes {
		nodeData = append(nodeData, map[string]interface{}{
			"id":         node.ID,
			"type":       cleanString(node.Type),
			"properties": node.Properties,
		})
	}

	// Prepare parameters
	params := map[string]interface{}{
		"nodes": nodeData,
	}

	if opts.IncludeSource {
		params["document_id"] = generateDocumentID(doc.Source)
		params["document_text"] = doc.Source.PageContent
		params["document_metadata"] = doc.Source.Metadata
	}

	// Execute query
	_, err := n.Query(ctx, query, params)
	if err != nil && isAPOCError(err) {
		return wrapAPOCError(err)
	}
	return err
}

// importRelationships imports relationships from a graph document
func (n *Neo4j) importRelationships(ctx context.Context, doc graphs.GraphDocument, opts *graphs.Options) error {
	if len(doc.Relationships) == 0 {
		return nil
	}

	// Generate query using the appropriate method
	query := n.getRelImportQuery()

	// Prepare relationship data
	var relData []map[string]interface{}
	for _, rel := range doc.Relationships {
		relData = append(relData, map[string]interface{}{
			"source":       rel.Source.ID,
			"source_label": cleanString(rel.Source.Type),
			"target":       rel.Target.ID,
			"target_label": cleanString(rel.Target.Type),
			"type":         cleanString(strings.ReplaceAll(strings.ToUpper(rel.Type), " ", "_")),
			"properties":   rel.Properties,
		})
	}

	params := map[string]interface{}{
		"relationships": relData,
	}

	// Execute query
	_, err := n.Query(ctx, query, params)
	if err != nil && isAPOCError(err) {
		return wrapAPOCError(err)
	}
	return err
}

// getNodeImportQuery generates the appropriate node import query based on base entity label setting
func (n *Neo4j) getNodeImportQuery(includeSource bool) string {
	var queryParts []string

	// Include source document if requested
	if includeSource {
		queryParts = append(queryParts,
			"MERGE (d:Document {id: $document_id})",
			"SET d.text = $document_text",
			"SET d += $document_metadata",
			"WITH d")
	}

	queryParts = append(queryParts, "UNWIND $nodes AS node")

	if n.baseEntityLabel {
		// Use base entity label approach
		queryParts = append(queryParts,
			fmt.Sprintf("MERGE (source:`%s` {id: node.id})", BASE_ENTITY_LABEL))
		queryParts = append(queryParts, "SET source += node.properties")
		if includeSource {
			queryParts = append(queryParts, "WITH source, node, d")
		} else {
			queryParts = append(queryParts, "WITH source, node")
		}
		queryParts = append(queryParts, "CALL apoc.create.addLabels(source, [node.type]) YIELD node AS n")
	} else {
		// Use dynamic labels approach
		if includeSource {
			queryParts = append(queryParts, "WITH d, node")
		}
		queryParts = append(queryParts, "CALL apoc.merge.node([node.type], {id: node.id}, node.properties, {}) YIELD node AS n")
	}

	if includeSource {
		queryParts = append(queryParts, "WITH d, n")
		queryParts = append(queryParts, "MERGE (d)-[:MENTIONS]->(n)")
	}

	queryParts = append(queryParts, "RETURN count(n) AS nodes_created")

	return strings.Join(queryParts, " ")
}

// getRelImportQuery generates the appropriate relationship import query based on base entity label setting
func (n *Neo4j) getRelImportQuery() string {
	if n.baseEntityLabel {
		return fmt.Sprintf("UNWIND $relationships AS rel "+
			"MERGE (source:%s {id: rel.source}) "+
			"MERGE (target:%s {id: rel.target}) "+
			"WITH source, target, rel "+
			"CALL apoc.merge.relationship(source, rel.type, {}, rel.properties, target) YIELD rel AS r "+
			"RETURN count(r) AS relationships_created", BASE_ENTITY_LABEL, BASE_ENTITY_LABEL)
	} else {
		return "UNWIND $relationships AS rel " +
			"CALL apoc.merge.node([rel.source_label], {id: rel.source}, {}, {}) YIELD node AS source " +
			"CALL apoc.merge.node([rel.target_label], {id: rel.target}, {}, {}) YIELD node AS target " +
			"CALL apoc.merge.relationship(source, rel.type, {}, rel.properties, target) YIELD rel AS r " +
			"RETURN count(r) AS relationships_created"
	}
}

// getSessionConfig returns the session configuration for this Neo4j instance
func (n *Neo4j) getSessionConfig() neo4j.SessionConfig {
	return neo4j.SessionConfig{DatabaseName: n.database}
}

// getNodeAddQuery generates the appropriate node addition query based on merge mode
func (n *Neo4j) getNodeAddQuery(mode graphs.MergeMode) string {
	switch mode {
	case graphs.MergeModeCreate:
		if n.baseEntityLabel {
			return fmt.Sprintf("CREATE (n:`%s`:`%s` {id: $id, type: $type}) SET n += $properties", BASE_ENTITY_LABEL, "$type")
		}
		return "CREATE (n {id: $id}) SET n:($type) SET n += $properties"
	case graphs.MergeModeUpdate:
		return "MATCH (n {id: $id}) SET n += $properties"
	case graphs.MergeModeReplace:
		if n.baseEntityLabel {
			return fmt.Sprintf("MERGE (n:`%s` {id: $id}) SET n:$type SET n = $properties", BASE_ENTITY_LABEL)
		}
		return "MERGE (n {id: $id}) SET n:($type) SET n = $properties"
	default: // MergeModeUpsert
		if n.baseEntityLabel {
			return fmt.Sprintf("MERGE (n:`%s` {id: $id}) SET n:$type SET n += $properties", BASE_ENTITY_LABEL)
		}
		return "MERGE (n {id: $id}) SET n:($type) SET n += $properties"
	}
}

// getRelationshipAddQuery generates the appropriate relationship addition query based on merge mode
func (n *Neo4j) getRelationshipAddQuery(mode graphs.MergeMode) string {
	switch mode {
	case graphs.MergeModeCreate:
		return `
			MATCH (s {id: $sourceId}), (t {id: $targetId})
			CREATE (s)-[r:` + "`$relType`" + `]->(t)
			SET r = $properties`
	case graphs.MergeModeUpdate:
		return `
			MATCH (s {id: $sourceId})-[r:` + "`$relType`" + `]->(t {id: $targetId})
			SET r += $properties`
	case graphs.MergeModeReplace:
		return `
			MATCH (s {id: $sourceId}), (t {id: $targetId})
			MERGE (s)-[r:` + "`$relType`" + `]->(t)
			SET r = $properties`
	default: // MergeModeUpsert
		return `
			MATCH (s {id: $sourceId}), (t {id: $targetId})
			MERGE (s)-[r:` + "`$relType`" + `]->(t)
			SET r += $properties`
	}
}

// ensureBaseEntityConstraint creates the base entity constraint if needed
func (n *Neo4j) ensureBaseEntityConstraint(ctx context.Context) error {
	if !n.baseEntityLabel {
		return nil
	}

	// Check if constraint already exists
	constraintQuery := "SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties WHERE $label IN labelsOrTypes AND $property IN properties"
	result, err := n.Query(ctx, constraintQuery, map[string]interface{}{
		"label":    BASE_ENTITY_LABEL,
		"property": "id",
	})
	if err != nil {
		// Fallback: try to create constraint anyway
	} else if records, ok := result["records"].([]map[string]interface{}); ok && len(records) > 0 {
		// Constraint already exists
		return nil
	}

	// Create constraint
	createConstraintQuery := fmt.Sprintf("CREATE CONSTRAINT IF NOT EXISTS FOR (b:`%s`) REQUIRE b.id IS UNIQUE", BASE_ENTITY_LABEL)
	_, err = n.Query(ctx, createConstraintQuery, nil)
	return err
}

// AddNodes adds individual nodes to the Neo4j store
func (n *Neo4j) AddNodes(ctx context.Context, nodes []graphs.Node, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	for _, node := range nodes {
		var query string
		switch opts.MergeMode {
		case graphs.MergeModeCreate:
			if n.baseEntityLabel {
				query = fmt.Sprintf("CREATE (n:`%s`:`%s` {id: $id}) SET n += $properties", node.Type, BASE_ENTITY_LABEL)
			} else {
				query = fmt.Sprintf("CREATE (n:`%s` {id: $id}) SET n += $properties", node.Type)
			}
		case graphs.MergeModeUpdate:
			query = fmt.Sprintf("MATCH (n:`%s` {id: $id}) SET n += $properties", node.Type)
		case graphs.MergeModeReplace:
			if n.baseEntityLabel {
				query = fmt.Sprintf("MERGE (n:`%s`:`%s` {id: $id}) SET n = $properties", node.Type, BASE_ENTITY_LABEL)
			} else {
				query = fmt.Sprintf("MERGE (n:`%s` {id: $id}) SET n = $properties", node.Type)
			}
		default: // MergeModeUpsert
			if n.baseEntityLabel {
				query = fmt.Sprintf("MERGE (n:`%s`:`%s` {id: $id}) SET n += $properties", node.Type, BASE_ENTITY_LABEL)
			} else {
				query = fmt.Sprintf("MERGE (n:`%s` {id: $id}) SET n += $properties", node.Type)
			}
		}

		params := map[string]interface{}{
			"id":         node.ID,
			"properties": node.Properties,
		}

		if _, err := session.Run(ctx, query, params); err != nil {
			return fmt.Errorf("failed to add node %s: %w", node.ID, err)
		}
	}

	return nil
}

// AddRelationships adds individual relationships to the Neo4j store
func (n *Neo4j) AddRelationships(ctx context.Context, relationships []graphs.Relationship, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	for _, rel := range relationships {
		var query string
		switch opts.MergeMode {
		case graphs.MergeModeCreate:
			query = fmt.Sprintf(`
				MATCH (s {id: $sourceId}), (t {id: $targetId})
				CREATE (s)-[r:%s]->(t)
				SET r = $properties
			`, rel.Type)
		case graphs.MergeModeUpdate:
			query = fmt.Sprintf(`
				MATCH (s {id: $sourceId})-[r:%s]->(t {id: $targetId})
				SET r += $properties
			`, rel.Type)
		case graphs.MergeModeReplace:
			query = fmt.Sprintf(`
				MATCH (s {id: $sourceId}), (t {id: $targetId})
				MERGE (s)-[r:%s]->(t)
				SET r = $properties
			`, rel.Type)
		default: // MergeModeUpsert
			query = fmt.Sprintf(`
				MATCH (s {id: $sourceId}), (t {id: $targetId})
				MERGE (s)-[r:%s]->(t)
				SET r += $properties
			`, rel.Type)
		}

		params := map[string]interface{}{
			"sourceId":   rel.Source.ID,
			"targetId":   rel.Target.ID,
			"properties": rel.Properties,
		}

		if _, err := session.Run(ctx, query, params); err != nil {
			return fmt.Errorf("failed to add relationship %s-%s->%s: %w",
				rel.Source.ID, rel.Type, rel.Target.ID, err)
		}
	}

	return nil
}
