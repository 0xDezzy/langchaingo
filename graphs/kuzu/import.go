package kuzu

import (
	"context"
	"fmt"

	"github.com/tmc/langchaingo/graphs"
)

// addSourceDocument adds a source document as a Chunk node
func (k *Kuzu) addSourceDocument(doc graphs.GraphDocument) error {
	// Check if source document is empty
	if doc.Source.PageContent == "" {
		return nil // No meaningful source document to add
	}

	// Generate or use existing document ID
	docID := ""
	if id, exists := doc.Source.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok {
			docID = idStr
		}
	}

	// If no ID provided, generate one from content hash
	if docID == "" {
		docID = generateDocumentID(doc.Source.PageContent)
	}

	// Use MERGE to avoid duplicates and handle upserts
	query := `
		MERGE (c:Chunk {id: $id})
		SET c.text = $text,
		    c.type = "text_chunk"
	`

	params := map[string]interface{}{
		"id":   docID,
		"text": doc.Source.PageContent,
	}

	ctx := context.Background()
	_, err := k.Query(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to add source document: %w", err)
	}

	return nil
}

// addNode adds a single node to the database
func (k *Kuzu) addNode(node graphs.Node) error {
	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	// Create type converter for proper data type handling
	typeConverter := NewTypeConverter()
	propertyConverter := NewPropertyConverter()

	// Convert properties using type converter
	convertedProperties, err := propertyConverter.ConvertProperties(node.Properties)
	if err != nil {
		return fmt.Errorf("failed to convert node properties: %w", err)
	}

	// Convert properties to separate arrays for KuzuDB MAP function
	var propKeys []string
	var propValues []string
	for key, value := range convertedProperties {
		if key == "id" || key == "type" {
			continue // Skip reserved properties
		}

		propKeys = append(propKeys, key)

		// Convert value to string for MAP storage
		// Use type converter to ensure proper KuzuDB compatibility
		convertedValue, _, convErr := typeConverter.ConvertGoValueToKuzu(value)
		if convErr != nil {
			// Fall back to string conversion
			propValues = append(propValues, fmt.Sprintf("%v", value))
		} else {
			propValues = append(propValues, fmt.Sprintf("%v", convertedValue))
		}
	}

	params := map[string]interface{}{
		"id":        node.ID,
		"node_type": node.Type,
		"prop_keys": propKeys,
		"prop_vals": propValues,
	}

	// Use MERGE to handle upserts with MAP function for properties
	query := fmt.Sprintf(`
		MERGE (n:%s {id: $id})
		SET n.type = $node_type,
		    n.properties = map($prop_keys, $prop_vals)
	`, sanitizeIdentifier(node.Type))

	ctx := context.Background()
	_, err = k.Query(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to add node %s: %w", node.ID, err)
	}

	return nil
}

// linkNodeToSource creates a MENTIONS relationship between source and node
func (k *Kuzu) linkNodeToSource(node graphs.Node, doc graphs.GraphDocument) error {
	// Check if source document is empty
	if doc.Source.PageContent == "" {
		return nil // No meaningful source to link to
	}

	// Get document ID
	docID := ""
	if id, exists := doc.Source.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok {
			docID = idStr
		}
	}

	if docID == "" {
		docID = generateDocumentID(doc.Source.PageContent)
	}

	// Create MENTIONS relationship
	query := `
		MATCH (c:Chunk {id: $doc_id})
		MATCH (n:%s {id: $node_id})
		MERGE (c)-[:MENTIONS]->(n)
	`

	// Need to format the query with the node type
	formattedQuery := fmt.Sprintf(query, sanitizeIdentifier(node.Type))

	params := map[string]interface{}{
		"doc_id":  docID,
		"node_id": node.ID,
	}

	ctx := context.Background()
	_, err := k.Query(ctx, formattedQuery, params)
	if err != nil {
		return fmt.Errorf("failed to link node %s to source: %w", node.ID, err)
	}

	return nil
}

// addRelationship adds a single relationship to the database
func (k *Kuzu) addRelationship(rel graphs.Relationship) error {
	if rel.Source.ID == "" || rel.Target.ID == "" {
		return fmt.Errorf("relationship source and target IDs cannot be empty")
	}

	// Create type converter for proper data type handling
	typeConverter := NewTypeConverter()
	propertyConverter := NewPropertyConverter()

	// Convert properties using type converter
	convertedProperties, err := propertyConverter.ConvertProperties(rel.Properties)
	if err != nil {
		return fmt.Errorf("failed to convert relationship properties: %w", err)
	}

	// Convert properties to separate arrays for KuzuDB MAP function
	var propKeys []string
	var propValues []string
	for key, value := range convertedProperties {
		propKeys = append(propKeys, key)

		// Convert value to string for MAP storage
		// Use type converter to ensure proper KuzuDB compatibility
		convertedValue, _, convErr := typeConverter.ConvertGoValueToKuzu(value)
		if convErr != nil {
			// Fall back to string conversion
			propValues = append(propValues, fmt.Sprintf("%v", value))
		} else {
			propValues = append(propValues, fmt.Sprintf("%v", convertedValue))
		}
	}

	params := map[string]interface{}{
		"source_id": rel.Source.ID,
		"target_id": rel.Target.ID,
		"prop_keys": propKeys,
		"prop_vals": propValues,
	}

	setClause := "SET r.properties = map($prop_keys, $prop_vals)"

	// Create relationship query
	query := fmt.Sprintf(`
		MATCH (s:%s {id: $source_id})
		MATCH (t:%s {id: $target_id})
		MERGE (s)-[r:%s]->(t)
		%s
	`,
		sanitizeIdentifier(rel.Source.Type),
		sanitizeIdentifier(rel.Target.Type),
		sanitizeIdentifier(rel.Type),
		setClause,
	)

	ctx := context.Background()
	_, err = k.Query(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to add relationship %s: %w", rel.Type, err)
	}

	return nil
}

// generateDocumentID generates a document ID from content
func generateDocumentID(content string) string {
	// Simple hash-based ID generation
	// In a real implementation, you might want to use MD5 or SHA256
	hash := 0
	for _, char := range content {
		hash = hash*31 + int(char)
	}
	return fmt.Sprintf("doc_%d", hash)
}

// batchAddNodes adds multiple nodes in a batch for better performance
func (k *Kuzu) batchAddNodes(nodes []graphs.Node) error {
	// For now, add nodes individually
	// TODO: Implement true batch processing when KuzuDB supports it
	for _, node := range nodes {
		if err := k.addNode(node); err != nil {
			return err
		}
	}
	return nil
}

// batchAddRelationships adds multiple relationships in a batch for better performance
func (k *Kuzu) batchAddRelationships(relationships []graphs.Relationship) error {
	// For now, add relationships individually
	// TODO: Implement true batch processing when KuzuDB supports it
	for _, rel := range relationships {
		if err := k.addRelationship(rel); err != nil {
			return err
		}
	}
	return nil
}

// validateGraphDocument performs basic validation on a graph document
func (k *Kuzu) validateGraphDocument(doc graphs.GraphDocument) error {
	// Check for empty node IDs
	for i, node := range doc.Nodes {
		if node.ID == "" {
			return fmt.Errorf("node at index %d has empty ID", i)
		}
		if node.Type == "" {
			return fmt.Errorf("node %s has empty type", node.ID)
		}
	}

	// Check for invalid relationships
	for i, rel := range doc.Relationships {
		if rel.Type == "" {
			return fmt.Errorf("relationship at index %d has empty type", i)
		}
		if rel.Source.ID == "" {
			return fmt.Errorf("relationship at index %d has empty source ID", i)
		}
		if rel.Target.ID == "" {
			return fmt.Errorf("relationship at index %d has empty target ID", i)
		}
	}

	return nil
}

// batchInsertNodesByType inserts multiple nodes of the same type efficiently
func (k *Kuzu) batchInsertNodesByType(ctx context.Context, nodeType string, nodes []graphs.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// For now, batch in groups of 100 for memory efficiency
	batchSize := 100
	for i := 0; i < len(nodes); i += batchSize {
		end := i + batchSize
		if end > len(nodes) {
			end = len(nodes)
		}

		batch := nodes[i:end]
		if err := k.insertNodeBatch(ctx, nodeType, batch); err != nil {
			return fmt.Errorf("failed to insert batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// insertNodeBatch inserts a batch of nodes using a single query
func (k *Kuzu) insertNodeBatch(ctx context.Context, nodeType string, nodes []graphs.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// Build batch UNWIND query for better performance
	// KuzuDB supports UNWIND for batch operations
	query := fmt.Sprintf(`
		UNWIND $nodes AS nodeData
		MERGE (n:%s {id: nodeData.id})
		SET n.type = nodeData.type
	`, sanitizeIdentifier(nodeType))

	// Handle additional properties if they exist
	if len(nodes) > 0 && len(nodes[0].Properties) > 0 {
		// Add property setting for common properties
		// This is a simplified approach - could be enhanced for dynamic properties
		query += `
		SET n.properties = nodeData.properties
		`
	}

	// Prepare node data
	nodeData := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		nodeData[i] = map[string]interface{}{
			"id":         node.ID,
			"type":       node.Type,
			"properties": node.Properties,
		}
	}

	params := map[string]interface{}{
		"nodes": nodeData,
	}

	_, err := k.Query(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert: %w", err)
	}

	return nil
}

// batchLinkNodesToSource creates MENTIONS relationships in batch
func (k *Kuzu) batchLinkNodesToSource(ctx context.Context, nodes []graphs.Node, doc graphs.GraphDocument) error {
	if doc.Source.PageContent == "" {
		return nil
	}

	// Get document ID
	docID := ""
	if id, exists := doc.Source.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok {
			docID = idStr
		}
	}

	if docID == "" {
		docID = generateDocumentID(doc.Source.PageContent)
	}

	// Group nodes by type for batch processing
	nodesByType := make(map[string][]string)
	for _, node := range nodes {
		nodesByType[node.Type] = append(nodesByType[node.Type], node.ID)
	}

	// Create MENTIONS relationships for each node type
	for nodeType, nodeIDs := range nodesByType {
		if err := k.batchCreateMentionsRelationships(ctx, docID, nodeType, nodeIDs); err != nil {
			return fmt.Errorf("failed to create MENTIONS for type %s: %w", nodeType, err)
		}
	}

	return nil
}

// batchCreateMentionsRelationships creates MENTIONS relationships for a node type
func (k *Kuzu) batchCreateMentionsRelationships(ctx context.Context, docID, nodeType string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	// Create batch MENTIONS relationships
	query := fmt.Sprintf(`
		MATCH (c:Chunk {id: $doc_id})
		UNWIND $node_ids AS nodeId
		MATCH (n:%s {id: nodeId})
		MERGE (c)-[:MENTIONS]->(n)
	`, sanitizeIdentifier(nodeType))

	params := map[string]interface{}{
		"doc_id":   docID,
		"node_ids": nodeIDs,
	}

	_, err := k.Query(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to create batch MENTIONS relationships: %w", err)
	}

	return nil
}

// batchInsertRelationships inserts multiple relationships efficiently
func (k *Kuzu) batchInsertRelationships(ctx context.Context, relationships []graphs.Relationship) error {
	if len(relationships) == 0 {
		return nil
	}

	// Process in batches for memory efficiency
	batchSize := 100
	for i := 0; i < len(relationships); i += batchSize {
		end := i + batchSize
		if end > len(relationships) {
			end = len(relationships)
		}

		batch := relationships[i:end]
		if err := k.insertRelationshipBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to insert relationship batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// insertRelationshipBatch inserts a batch of relationships using a single query
func (k *Kuzu) insertRelationshipBatch(ctx context.Context, relationships []graphs.Relationship) error {
	if len(relationships) == 0 {
		return nil
	}

	// Group relationships by pattern (source_type, rel_type, target_type)
	relGroups := make(map[string][]graphs.Relationship)
	for _, rel := range relationships {
		key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
		relGroups[key] = append(relGroups[key], rel)
	}

	// Process each relationship pattern separately
	for _, rels := range relGroups {
		if len(rels) == 0 {
			continue
		}

		// Use the first relationship to determine the pattern
		firstRel := rels[0]

		query := fmt.Sprintf(`
			UNWIND $relationships AS relData
			MATCH (s:%s {id: relData.source_id})
			MATCH (t:%s {id: relData.target_id})
			MERGE (s)-[r:%s]->(t)
		`,
			sanitizeIdentifier(firstRel.Source.Type),
			sanitizeIdentifier(firstRel.Target.Type),
			sanitizeIdentifier(firstRel.Type),
		)

		// Add properties if they exist
		if len(firstRel.Properties) > 0 {
			query += ` SET r.properties = relData.properties`
		}

		// Prepare relationship data
		relData := make([]map[string]interface{}, len(rels))
		for i, rel := range rels {
			relData[i] = map[string]interface{}{
				"source_id":  rel.Source.ID,
				"target_id":  rel.Target.ID,
				"properties": rel.Properties,
			}
		}

		params := map[string]interface{}{
			"relationships": relData,
		}

		_, err := k.Query(ctx, query, params)
		if err != nil {
			return fmt.Errorf("failed to insert relationship batch for pattern %s_%s_%s: %w",
				firstRel.Source.Type, firstRel.Type, firstRel.Target.Type, err)
		}
	}

	return nil
}

// AddGraphDocuments processes multiple graph documents with optimized batch handling
func (k *Kuzu) AddGraphDocuments(ctx context.Context, docs []graphs.GraphDocument, options ...graphs.Option) error {
	if len(docs) == 0 {
		return nil
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	// Use transaction for batch processing to ensure ACID properties
	return k.RunInTransaction(ctx, func(tx *Transaction) error {
		return k.addGraphDocumentsInTransaction(tx, docs, opts)
	}, WithReadOnly(false))
}

// addGraphDocumentsInTransaction processes documents within a transaction
func (k *Kuzu) addGraphDocumentsInTransaction(tx *Transaction, docs []graphs.GraphDocument, opts *graphs.Options) error {
	// Process documents in optimal batch sizes
	batchSize := 10 // Process 10 documents at a time
	for i := 0; i < len(docs); i += batchSize {
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		batch := docs[i:end]
		if err := k.processDocumentBatchInTransaction(tx, batch, opts); err != nil {
			return fmt.Errorf("failed to process document batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// AddGraphDocumentsWithTransaction processes documents using an existing transaction
func (k *Kuzu) AddGraphDocumentsWithTransaction(tx *Transaction, docs []graphs.GraphDocument, options ...graphs.Option) error {
	if len(docs) == 0 {
		return nil
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	return k.addGraphDocumentsInTransaction(tx, docs, opts)
}

// processDocumentBatchInTransaction processes a batch of documents within a transaction
func (k *Kuzu) processDocumentBatchInTransaction(tx *Transaction, docs []graphs.GraphDocument, opts *graphs.Options) error {
	// Validate all documents first
	for i, doc := range docs {
		if err := k.validateGraphDocument(doc); err != nil {
			return fmt.Errorf("document %d validation failed: %w", i, err)
		}
	}

	// Collect all unique node and relationship types across documents
	allNodeTypes := make(map[string]bool)
	allRelPatterns := make(map[string]struct {
		sourceType string
		targetType string
		relType    string
	})

	for _, doc := range docs {
		// Collect node types
		for _, node := range doc.Nodes {
			allNodeTypes[node.Type] = true
		}

		// Collect relationship patterns
		for _, rel := range doc.Relationships {
			key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
			allRelPatterns[key] = struct {
				sourceType string
				targetType string
				relType    string
			}{
				sourceType: rel.Source.Type,
				targetType: rel.Target.Type,
				relType:    rel.Type,
			}
		}
	}

	// Create all required tables upfront
	if err := k.ensureTablesForBatch(allNodeTypes, allRelPatterns); err != nil {
		return fmt.Errorf("failed to create tables for batch: %w", err)
	}

	// Process each document using transaction queries
	for _, doc := range docs {
		if err := k.processGraphDocumentInTransaction(tx, doc, opts); err != nil {
			return fmt.Errorf("failed to process document: %w", err)
		}
	}

	return nil
}

// processGraphDocumentInTransaction processes a single document within a transaction
func (k *Kuzu) processGraphDocumentInTransaction(tx *Transaction, doc graphs.GraphDocument, opts *graphs.Options) error {
	// Create tables for nodes and relationships first
	if err := k.ensureTablesExist(doc); err != nil {
		return err
	}

	// Add source document if required
	// TODO: Fix Chunk table and MENTIONS functionality for KuzuDB
	if opts.IncludeSource {
		// Skip source document functionality for now
		// if err := k.addSourceDocumentInTransaction(tx, doc); err != nil {
		//	return err
		// }
	}

	// Use batch processing for better performance within transaction
	if len(doc.Nodes) > 10 || len(doc.Relationships) > 10 {
		return k.processBatchInTransaction(tx, doc, opts)
	}

	// For small documents, process individually within transaction
	return k.processIndividuallyInTransaction(tx, doc, opts)
}

// addSourceDocumentInTransaction adds a source document within a transaction
func (k *Kuzu) addSourceDocumentInTransaction(tx *Transaction, doc graphs.GraphDocument) error {
	if doc.Source.PageContent == "" {
		return nil // No meaningful source document to add
	}

	// Generate or use existing document ID
	docID := ""
	if id, exists := doc.Source.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok {
			docID = idStr
		}
	}

	// If no ID provided, generate one from content hash
	if docID == "" {
		docID = generateDocumentID(doc.Source.PageContent)
	}

	// Use MERGE to avoid duplicates and handle upserts
	query := `
		MERGE (c:Chunk {id: $id})
		SET c.text = $text,
		    c.type = "text_chunk"
	`

	params := map[string]interface{}{
		"id":   docID,
		"text": doc.Source.PageContent,
	}

	_, err := tx.Query(query, params)
	if err != nil {
		return fmt.Errorf("failed to add source document in transaction: %w", err)
	}

	return nil
}

// processBatchInTransaction handles large documents with batch processing within transaction
func (k *Kuzu) processBatchInTransaction(tx *Transaction, doc graphs.GraphDocument, opts *graphs.Options) error {
	// Group nodes by type for efficient batch insertion
	nodesByType := make(map[string][]graphs.Node)
	for _, node := range doc.Nodes {
		nodesByType[node.Type] = append(nodesByType[node.Type], node)
	}

	// Batch insert nodes by type within transaction
	for nodeType, nodes := range nodesByType {
		if err := k.batchInsertNodesByTypeInTransaction(tx, nodeType, nodes); err != nil {
			return fmt.Errorf("failed to batch insert nodes of type %s: %w", nodeType, err)
		}
	}

	// Create source links if required
	// TODO: Fix MENTIONS table creation for KuzuDB
	if opts.IncludeSource {
		// Skip MENTIONS functionality for now
		// if err := k.batchLinkNodesToSourceInTransaction(tx, doc.Nodes, doc); err != nil {
		//	return fmt.Errorf("failed to batch link nodes to source: %w", err)
		// }
	}

	// Group relationships by type for batch insertion
	relsByType := make(map[string][]graphs.Relationship)
	for _, rel := range doc.Relationships {
		key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
		relsByType[key] = append(relsByType[key], rel)
	}

	// Batch insert relationships by type within transaction
	for _, rels := range relsByType {
		if err := k.batchInsertRelationshipsInTransaction(tx, rels); err != nil {
			return fmt.Errorf("failed to batch insert relationships: %w", err)
		}
	}

	return nil
}

// processIndividuallyInTransaction handles small documents with individual processing within transaction
func (k *Kuzu) processIndividuallyInTransaction(tx *Transaction, doc graphs.GraphDocument, opts *graphs.Options) error {
	// Add nodes
	for _, node := range doc.Nodes {
		if err := k.addNodeInTransaction(tx, node); err != nil {
			return err
		}

		// Link to source document if required
		// TODO: Fix MENTIONS table creation for KuzuDB
		// KuzuDB doesn't support generic Node types, so we need specific MENTIONS tables
		// For now, we'll skip this functionality
		if opts.IncludeSource {
			// Skip MENTIONS functionality for now
			// if err := k.linkNodeToSource(node, doc); err != nil {
			//	return err
			// }
		}
	}

	// Add relationships
	for _, rel := range doc.Relationships {
		if err := k.addRelationshipInTransaction(tx, rel); err != nil {
			return err
		}
	}

	return nil
}

// Transaction-aware helper methods

// addNodeInTransaction adds a single node within a transaction
func (k *Kuzu) addNodeInTransaction(tx *Transaction, node graphs.Node) error {
	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	// Convert properties to separate arrays for KuzuDB MAP function
	var propKeys []string
	var propValues []string
	for key, value := range node.Properties {
		if key == "id" || key == "type" {
			continue // Skip reserved properties
		}
		propKeys = append(propKeys, key)
		// Convert value to string for MAP storage
		propValues = append(propValues, fmt.Sprintf("%v", value))
	}

	params := map[string]interface{}{
		"id":        node.ID,
		"node_type": node.Type,
		"prop_keys": propKeys,
		"prop_vals": propValues,
	}

	// Use MERGE to handle upserts with MAP function for properties
	query := fmt.Sprintf(`
		MERGE (n:%s {id: $id})
		SET n.type = $node_type,
		    n.properties = map($prop_keys, $prop_vals)
	`, sanitizeIdentifier(node.Type))

	_, err := tx.Query(query, params)
	if err != nil {
		return fmt.Errorf("failed to add node %s in transaction: %w", node.ID, err)
	}

	return nil
}

// linkNodeToSourceInTransaction creates a MENTIONS relationship within transaction
func (k *Kuzu) linkNodeToSourceInTransaction(tx *Transaction, node graphs.Node, doc graphs.GraphDocument) error {
	if doc.Source.PageContent == "" {
		return nil // No source to link to
	}

	// Get document ID
	docID := ""
	if id, exists := doc.Source.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok {
			docID = idStr
		}
	}

	if docID == "" {
		docID = generateDocumentID(doc.Source.PageContent)
	}

	// Create MENTIONS relationship
	query := `
		MATCH (c:Chunk {id: $doc_id})
		MATCH (n:%s {id: $node_id})
		MERGE (c)-[:MENTIONS]->(n)
	`

	// Need to format the query with the node type
	formattedQuery := fmt.Sprintf(query, sanitizeIdentifier(node.Type))

	params := map[string]interface{}{
		"doc_id":  docID,
		"node_id": node.ID,
	}

	_, err := tx.Query(formattedQuery, params)
	if err != nil {
		return fmt.Errorf("failed to link node %s to source in transaction: %w", node.ID, err)
	}

	return nil
}

// addRelationshipInTransaction adds a single relationship within transaction
func (k *Kuzu) addRelationshipInTransaction(tx *Transaction, rel graphs.Relationship) error {
	if rel.Source.ID == "" || rel.Target.ID == "" {
		return fmt.Errorf("relationship source and target IDs cannot be empty")
	}

	// Convert properties to separate arrays for KuzuDB MAP function
	var propKeys []string
	var propValues []string
	for key, value := range rel.Properties {
		propKeys = append(propKeys, key)
		// Convert value to string for MAP storage
		propValues = append(propValues, fmt.Sprintf("%v", value))
	}

	params := map[string]interface{}{
		"source_id": rel.Source.ID,
		"target_id": rel.Target.ID,
		"prop_keys": propKeys,
		"prop_vals": propValues,
	}

	setClause := "SET r.properties = map($prop_keys, $prop_vals)"

	// Create relationship query
	query := fmt.Sprintf(`
		MATCH (s:%s {id: $source_id})
		MATCH (t:%s {id: $target_id})
		MERGE (s)-[r:%s]->(t)
		%s
	`,
		sanitizeIdentifier(rel.Source.Type),
		sanitizeIdentifier(rel.Target.Type),
		sanitizeIdentifier(rel.Type),
		setClause,
	)

	_, err := tx.Query(query, params)
	if err != nil {
		return fmt.Errorf("failed to add relationship %s in transaction: %w", rel.Type, err)
	}

	return nil
}

// Batch methods within transaction

// batchInsertNodesByTypeInTransaction inserts multiple nodes within transaction
func (k *Kuzu) batchInsertNodesByTypeInTransaction(tx *Transaction, nodeType string, nodes []graphs.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// For now, batch in groups of 100 for memory efficiency
	batchSize := 100
	for i := 0; i < len(nodes); i += batchSize {
		end := i + batchSize
		if end > len(nodes) {
			end = len(nodes)
		}

		batch := nodes[i:end]
		if err := k.insertNodeBatchInTransaction(tx, nodeType, batch); err != nil {
			return fmt.Errorf("failed to insert batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// insertNodeBatchInTransaction inserts a batch of nodes within transaction
func (k *Kuzu) insertNodeBatchInTransaction(tx *Transaction, nodeType string, nodes []graphs.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// Build batch UNWIND query for better performance
	query := fmt.Sprintf(`
		UNWIND $nodes AS nodeData
		MERGE (n:%s {id: nodeData.id})
		SET n.type = nodeData.type
	`, sanitizeIdentifier(nodeType))

	// Handle additional properties if they exist
	if len(nodes) > 0 && len(nodes[0].Properties) > 0 {
		query += `
		SET n.properties = nodeData.properties
		`
	}

	// Prepare node data
	nodeData := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		nodeData[i] = map[string]interface{}{
			"id":         node.ID,
			"type":       node.Type,
			"properties": node.Properties,
		}
	}

	params := map[string]interface{}{
		"nodes": nodeData,
	}

	_, err := tx.Query(query, params)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert in transaction: %w", err)
	}

	return nil
}

// batchLinkNodesToSourceInTransaction creates MENTIONS relationships within transaction
func (k *Kuzu) batchLinkNodesToSourceInTransaction(tx *Transaction, nodes []graphs.Node, doc graphs.GraphDocument) error {
	if doc.Source.PageContent == "" {
		return nil
	}

	// Get document ID
	docID := ""
	if id, exists := doc.Source.Metadata["id"]; exists {
		if idStr, ok := id.(string); ok {
			docID = idStr
		}
	}

	if docID == "" {
		docID = generateDocumentID(doc.Source.PageContent)
	}

	// Group nodes by type for batch processing
	nodesByType := make(map[string][]string)
	for _, node := range nodes {
		nodesByType[node.Type] = append(nodesByType[node.Type], node.ID)
	}

	// Create MENTIONS relationships for each node type
	for nodeType, nodeIDs := range nodesByType {
		if err := k.batchCreateMentionsRelationshipsInTransaction(tx, docID, nodeType, nodeIDs); err != nil {
			return fmt.Errorf("failed to create MENTIONS for type %s: %w", nodeType, err)
		}
	}

	return nil
}

// batchCreateMentionsRelationshipsInTransaction creates MENTIONS within transaction
func (k *Kuzu) batchCreateMentionsRelationshipsInTransaction(tx *Transaction, docID, nodeType string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	// Create batch MENTIONS relationships
	query := fmt.Sprintf(`
		MATCH (c:Chunk {id: $doc_id})
		UNWIND $node_ids AS nodeId
		MATCH (n:%s {id: nodeId})
		MERGE (c)-[:MENTIONS]->(n)
	`, sanitizeIdentifier(nodeType))

	params := map[string]interface{}{
		"doc_id":   docID,
		"node_ids": nodeIDs,
	}

	_, err := tx.Query(query, params)
	if err != nil {
		return fmt.Errorf("failed to create batch MENTIONS relationships in transaction: %w", err)
	}

	return nil
}

// batchInsertRelationshipsInTransaction inserts relationships within transaction
func (k *Kuzu) batchInsertRelationshipsInTransaction(tx *Transaction, relationships []graphs.Relationship) error {
	if len(relationships) == 0 {
		return nil
	}

	// Process in batches for memory efficiency
	batchSize := 100
	for i := 0; i < len(relationships); i += batchSize {
		end := i + batchSize
		if end > len(relationships) {
			end = len(relationships)
		}

		batch := relationships[i:end]
		if err := k.insertRelationshipBatchInTransaction(tx, batch); err != nil {
			return fmt.Errorf("failed to insert relationship batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// insertRelationshipBatchInTransaction inserts relationships within transaction
func (k *Kuzu) insertRelationshipBatchInTransaction(tx *Transaction, relationships []graphs.Relationship) error {
	if len(relationships) == 0 {
		return nil
	}

	// Group relationships by pattern (source_type, rel_type, target_type)
	relGroups := make(map[string][]graphs.Relationship)
	for _, rel := range relationships {
		key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
		relGroups[key] = append(relGroups[key], rel)
	}

	// Process each relationship pattern separately
	for _, rels := range relGroups {
		if len(rels) == 0 {
			continue
		}

		// Use the first relationship to determine the pattern
		firstRel := rels[0]

		query := fmt.Sprintf(`
			UNWIND $relationships AS relData
			MATCH (s:%s {id: relData.source_id})
			MATCH (t:%s {id: relData.target_id})
			MERGE (s)-[r:%s]->(t)
		`,
			sanitizeIdentifier(firstRel.Source.Type),
			sanitizeIdentifier(firstRel.Target.Type),
			sanitizeIdentifier(firstRel.Type),
		)

		// Add properties if they exist
		if len(firstRel.Properties) > 0 {
			query += ` SET r.properties = relData.properties`
		}

		// Prepare relationship data
		relData := make([]map[string]interface{}, len(rels))
		for i, rel := range rels {
			relData[i] = map[string]interface{}{
				"source_id":  rel.Source.ID,
				"target_id":  rel.Target.ID,
				"properties": rel.Properties,
			}
		}

		params := map[string]interface{}{
			"relationships": relData,
		}

		_, err := tx.Query(query, params)
		if err != nil {
			return fmt.Errorf("failed to insert relationship batch for pattern %s_%s_%s in transaction: %w",
				firstRel.Source.Type, firstRel.Type, firstRel.Target.Type, err)
		}
	}

	return nil
}

// processDocumentBatch processes a batch of documents together for efficiency
func (k *Kuzu) processDocumentBatch(ctx context.Context, docs []graphs.GraphDocument, opts *graphs.Options) error {
	// Validate all documents first
	for i, doc := range docs {
		if err := k.validateGraphDocument(doc); err != nil {
			return fmt.Errorf("document %d validation failed: %w", i, err)
		}
	}

	// Collect all unique node and relationship types across documents
	allNodeTypes := make(map[string]bool)
	allRelPatterns := make(map[string]struct {
		sourceType string
		targetType string
		relType    string
	})

	for _, doc := range docs {
		// Collect node types
		for _, node := range doc.Nodes {
			allNodeTypes[node.Type] = true
		}

		// Collect relationship patterns
		for _, rel := range doc.Relationships {
			key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
			allRelPatterns[key] = struct {
				sourceType string
				targetType string
				relType    string
			}{
				sourceType: rel.Source.Type,
				targetType: rel.Target.Type,
				relType:    rel.Type,
			}
		}
	}

	// Create all required tables upfront
	if err := k.ensureTablesForBatch(allNodeTypes, allRelPatterns); err != nil {
		return fmt.Errorf("failed to create tables for batch: %w", err)
	}

	// Process each document
	for _, doc := range docs {
		if err := k.processGraphDocument(ctx, doc, opts); err != nil {
			return fmt.Errorf("failed to process document: %w", err)
		}
	}

	return nil
}

// ensureTablesForBatch creates all required tables for a batch of documents
func (k *Kuzu) ensureTablesForBatch(nodeTypes map[string]bool, relPatterns map[string]struct {
	sourceType string
	targetType string
	relType    string
}) error {
	// Create node tables
	for nodeType := range nodeTypes {
		if err := k.createNodeTable(nodeType); err != nil {
			return fmt.Errorf("failed to create node table %s: %w", nodeType, err)
		}
	}

	// Create Chunk table for source tracking
	if err := k.createChunkNodeTable(); err != nil {
		return fmt.Errorf("failed to create Chunk table: %w", err)
	}

	// Create relationship tables
	for _, relInfo := range relPatterns {
		if err := k.createRelationshipTable(relInfo.relType, relInfo.sourceType, relInfo.targetType); err != nil {
			return fmt.Errorf("failed to create relationship table %s: %w", relInfo.relType, err)
		}
	}

	// Create MENTIONS relationship table
	if err := k.createMentionsRelationshipTable(); err != nil {
		return fmt.Errorf("failed to create MENTIONS table: %w", err)
	}

	return nil
}

// DeduplicateNodes removes duplicate nodes within a document based on ID
func (k *Kuzu) DeduplicateNodes(nodes []graphs.Node) []graphs.Node {
	seen := make(map[string]bool)
	var result []graphs.Node

	for _, node := range nodes {
		if !seen[node.ID] {
			seen[node.ID] = true
			result = append(result, node)
		}
	}

	return result
}

// DeduplicateRelationships removes duplicate relationships based on source, target, and type
func (k *Kuzu) DeduplicateRelationships(relationships []graphs.Relationship) []graphs.Relationship {
	seen := make(map[string]bool)
	var result []graphs.Relationship

	for _, rel := range relationships {
		key := fmt.Sprintf("%s_%s_%s_%s", rel.Source.ID, rel.Type, rel.Target.ID, rel.Source.Type+"_"+rel.Target.Type)
		if !seen[key] {
			seen[key] = true
			result = append(result, rel)
		}
	}

	return result
}

// GetImportStatistics returns statistics about the import process
func (k *Kuzu) GetImportStatistics() map[string]interface{} {
	k.tablesMux.RLock()
	defer k.tablesMux.RUnlock()

	return map[string]interface{}{
		"node_tables_created":         len(k.nodeTables),
		"relationship_tables_created": len(k.relTables),
		"node_types":                  k.getNodeTypesList(),
		"relationship_types":          k.getRelationshipTypesList(),
	}
}

// getNodeTypesList returns a list of created node types
func (k *Kuzu) getNodeTypesList() []string {
	var types []string
	for nodeType := range k.nodeTables {
		types = append(types, nodeType)
	}
	return types
}

// getRelationshipTypesList returns a list of created relationship types
func (k *Kuzu) getRelationshipTypesList() []string {
	var types []string
	for relType := range k.relTables {
		types = append(types, relType)
	}
	return types
}
