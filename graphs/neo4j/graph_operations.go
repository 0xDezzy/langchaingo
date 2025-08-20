package neo4j

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/tmc/langchaingo/graphs"
)

// UpdateNode updates an existing node in the Neo4j store
func (n *Neo4j) UpdateNode(ctx context.Context, nodeID string, properties map[string]interface{}, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := `
		MATCH (n {id: $id})
		SET n += $properties
		RETURN n
	`
	params := map[string]interface{}{
		"id":         nodeID,
		"properties": properties,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to update node %s: %w", nodeID, err)
	}

	if !result.Next(ctx) {
		return fmt.Errorf("node %s not found", nodeID)
	}

	return nil
}

// UpdateRelationship updates an existing relationship in the Neo4j store
func (n *Neo4j) UpdateRelationship(ctx context.Context, sourceID, targetID, relType string, properties map[string]interface{}, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := fmt.Sprintf(`
		MATCH (s {id: $sourceId})-[r:%s]->(t {id: $targetId})
		SET r += $properties
		RETURN r
	`, relType)
	params := map[string]interface{}{
		"sourceId":   sourceID,
		"targetId":   targetID,
		"properties": properties,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to update relationship %s-%s->%s: %w", sourceID, relType, targetID, err)
	}

	if !result.Next(ctx) {
		return fmt.Errorf("relationship %s-%s->%s not found", sourceID, relType, targetID)
	}

	return nil
}

// RemoveNode removes a node and all its relationships from the Neo4j store
func (n *Neo4j) RemoveNode(ctx context.Context, nodeID string, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	var query string
	if opts.CascadeDelete {
		query = `
			MATCH (n {id: $id})
			DETACH DELETE n
		`
	} else {
		query = `
			MATCH (n {id: $id})
			WHERE NOT (n)--()
			DELETE n
		`
	}

	params := map[string]interface{}{
		"id": nodeID,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to remove node %s: %w", nodeID, err)
	}

	return nil
}

// RemoveNodes removes multiple nodes and their relationships from the Neo4j store
func (n *Neo4j) RemoveNodes(ctx context.Context, nodeIDs []string, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	var query string
	if opts.CascadeDelete {
		query = `
			UNWIND $ids AS id
			MATCH (n {id: id})
			DETACH DELETE n
		`
	} else {
		query = `
			UNWIND $ids AS id
			MATCH (n {id: id})
			WHERE NOT (n)--()
			DELETE n
		`
	}

	params := map[string]interface{}{
		"ids": nodeIDs,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to remove nodes: %w", err)
	}

	return nil
}

// RemoveRelationship removes a specific relationship from the Neo4j store
func (n *Neo4j) RemoveRelationship(ctx context.Context, sourceID, targetID, relType string, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := fmt.Sprintf(`
		MATCH (s {id: $sourceId})-[r:%s]->(t {id: $targetId})
		DELETE r
	`, relType)
	params := map[string]interface{}{
		"sourceId": sourceID,
		"targetId": targetID,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to remove relationship %s-%s->%s: %w", sourceID, relType, targetID, err)
	}

	return nil
}

// RemoveRelationships removes multiple relationships from the Neo4j store
func (n *Neo4j) RemoveRelationships(ctx context.Context, relationships []graphs.RelationshipIdentifier, options ...graphs.Option) error {
	if n.driver == nil {
		return ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	for _, rel := range relationships {
		if err := n.RemoveRelationship(ctx, rel.SourceID, rel.TargetID, rel.Type, options...); err != nil {
			return err
		}
	}

	return nil
}

// GetNode retrieves a node by its ID
func (n *Neo4j) GetNode(ctx context.Context, nodeID string, options ...graphs.Option) (*graphs.Node, error) {
	if n.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := "MATCH (n {id: $id}) RETURN n"
	params := map[string]interface{}{
		"id": nodeID,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", nodeID, err)
	}

	if !result.Next(ctx) {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	record := result.Record()
	nodeValue := record.Values[0]

	if node, ok := nodeValue.(neo4j.Node); ok {
		return n.convertNeo4jNodeToGraphNode(node), nil
	}

	return nil, fmt.Errorf("unexpected node type returned")
}

// GetNodes retrieves multiple nodes by their IDs
func (n *Neo4j) GetNodes(ctx context.Context, nodeIDs []string, options ...graphs.Option) ([]graphs.Node, error) {
	if n.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := "UNWIND $ids AS id MATCH (n {id: id}) RETURN n"
	params := map[string]interface{}{
		"ids": nodeIDs,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}

	var nodes []graphs.Node
	for result.Next(ctx) {
		record := result.Record()
		if len(record.Values) > 0 {
			nodeValue := record.Values[0]
			if node, ok := nodeValue.(neo4j.Node); ok {
				nodes = append(nodes, *n.convertNeo4jNodeToGraphNode(node))
			}
		}
	}

	return nodes, nil
}

// GetRelationships retrieves relationships between nodes
func (n *Neo4j) GetRelationships(ctx context.Context, sourceID, targetID string, relType string, options ...graphs.Option) ([]graphs.Relationship, error) {
	if n.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	var query string
	var params map[string]interface{}

	if relType != "" {
		query = fmt.Sprintf("MATCH (s {id: $sourceId})-[r:%s]->(t {id: $targetId}) RETURN s, r, t", relType)
		params = map[string]interface{}{
			"sourceId": sourceID,
			"targetId": targetID,
		}
	} else {
		query = "MATCH (s {id: $sourceId})-[r]->(t {id: $targetId}) RETURN s, r, t"
		params = map[string]interface{}{
			"sourceId": sourceID,
			"targetId": targetID,
		}
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get relationships: %w", err)
	}

	var relationships []graphs.Relationship
	for result.Next(ctx) {
		record := result.Record()
		sourceNodeVal, _ := record.Get("s")
		sourceNode := sourceNodeVal.(neo4j.Node)
		relationshipVal, _ := record.Get("r")
		relationship := relationshipVal.(neo4j.Relationship)
		targetNodeVal, _ := record.Get("t")
		targetNode := targetNodeVal.(neo4j.Node)

		rel := graphs.Relationship{
			Source:     *n.convertNeo4jNodeToGraphNode(sourceNode),
			Target:     *n.convertNeo4jNodeToGraphNode(targetNode),
			Type:       relationship.Type,
			Properties: relationship.Props,
		}
		relationships = append(relationships, rel)
	}

	return relationships, nil
}

// GetNodesByType retrieves all nodes of a specific type
func (n *Neo4j) GetNodesByType(ctx context.Context, nodeType string, options ...graphs.Option) ([]graphs.Node, error) {
	if n.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := fmt.Sprintf("MATCH (n:`%s`) RETURN n", nodeType)
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" SKIP %d", opts.Offset)
	}

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes by type %s: %w", nodeType, err)
	}

	var nodes []graphs.Node
	for result.Next(ctx) {
		record := result.Record()
		if len(record.Values) > 0 {
			nodeValue := record.Values[0]
			if node, ok := nodeValue.(neo4j.Node); ok {
				nodes = append(nodes, *n.convertNeo4jNodeToGraphNode(node))
			}
		}
	}

	return nodes, nil
}

// GetRelationshipsByType retrieves all relationships of a specific type
func (n *Neo4j) GetRelationshipsByType(ctx context.Context, relType string, options ...graphs.Option) ([]graphs.Relationship, error) {
	if n.driver == nil {
		return nil, ErrDriverNotInitialized
	}

	opts := graphs.NewOptions()
	for _, opt := range options {
		opt(opts)
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := fmt.Sprintf("MATCH (s)-[r:%s]->(t) RETURN s, r, t", relType)
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" SKIP %d", opts.Offset)
	}

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get relationships by type %s: %w", relType, err)
	}

	var relationships []graphs.Relationship
	for result.Next(ctx) {
		record := result.Record()
		sourceNodeVal, _ := record.Get("s")
		sourceNode := sourceNodeVal.(neo4j.Node)
		relationshipVal, _ := record.Get("r")
		relationship := relationshipVal.(neo4j.Relationship)
		targetNodeVal, _ := record.Get("t")
		targetNode := targetNodeVal.(neo4j.Node)

		rel := graphs.Relationship{
			Source:     *n.convertNeo4jNodeToGraphNode(sourceNode),
			Target:     *n.convertNeo4jNodeToGraphNode(targetNode),
			Type:       relationship.Type,
			Properties: relationship.Props,
		}
		relationships = append(relationships, rel)
	}

	return relationships, nil
}

// NodeExists checks if a node exists in the Neo4j store
func (n *Neo4j) NodeExists(ctx context.Context, nodeID string, options ...graphs.Option) (bool, error) {
	if n.driver == nil {
		return false, ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := "MATCH (n {id: $id}) RETURN count(n) > 0 as exists"
	params := map[string]interface{}{
		"id": nodeID,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return false, fmt.Errorf("failed to check node existence %s: %w", nodeID, err)
	}

	if result.Next(ctx) {
		record := result.Record()
		existsVal, _ := record.Get("exists")
		exists := existsVal.(bool)
		return exists, nil
	}

	return false, nil
}

// RelationshipExists checks if a relationship exists in the Neo4j store
func (n *Neo4j) RelationshipExists(ctx context.Context, sourceID, targetID, relType string, options ...graphs.Option) (bool, error) {
	if n.driver == nil {
		return false, ErrDriverNotInitialized
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.database})
	defer session.Close(ctx)

	query := fmt.Sprintf("MATCH (s {id: $sourceId})-[r:%s]->(t {id: $targetId}) RETURN count(r) > 0 as exists", relType)
	params := map[string]interface{}{
		"sourceId": sourceID,
		"targetId": targetID,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return false, fmt.Errorf("failed to check relationship existence: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		existsVal, _ := record.Get("exists")
		exists := existsVal.(bool)
		return exists, nil
	}

	return false, nil
}

// convertNeo4jNodeToGraphNode converts a Neo4j node to a graphs.Node
func (n *Neo4j) convertNeo4jNodeToGraphNode(node neo4j.Node) *graphs.Node {
	// Get the first label as the node type (Neo4j nodes can have multiple labels)
	var nodeType string
	if len(node.Labels) > 0 {
		// Skip the base entity label if present
		for _, label := range node.Labels {
			if label != BASE_ENTITY_LABEL {
				nodeType = label
				break
			}
		}
	}

	// Get node ID from properties
	nodeID := ""
	if id, ok := node.Props["id"]; ok {
		if idStr, ok := id.(string); ok {
			nodeID = idStr
		}
	}

	return &graphs.Node{
		ID:         nodeID,
		Type:       nodeType,
		Properties: node.Props,
	}
}

// GetStructuredSchema returns the structured schema information.
func (n *Neo4j) GetStructuredSchema() map[string]interface{} {
	n.schemaMux.RLock()
	defer n.schemaMux.RUnlock()
	return n.structuredSchema
}
