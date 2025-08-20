package graphs

import (
	"encoding/json"

	"github.com/tmc/langchaingo/schema"
)

// Node represents a node in a graph with associated properties.
type Node struct {
	// ID is the unique identifier for the node.
	ID string `json:"id"`
	// Type  is the type or label of the node.
	Type string `json:"type"`
	// Properties contains additional properties and metadata associated with the node.
	Properties map[string]interface{} `json:"properties,"`
}

// Relationship represents a directed relationship between two nodes in a graph.
type Relationship struct {
	// Source is the source node of the relationship
	Source Node `json:"source"`
	// Target is the target node of the relationship
	Target Node `json:"target"`
	// Type is the type of relationship
	Type string `json:"type"`
	// Properties contains additional properties associated with the relationship
	Properties map[string]interface{} `json:"properties"`
}

// GraphDocument represents a document consisting of nodes and relationships
type GraphDocument struct {
	// Nodes is a list of nodes in the graph
	Nodes []Node `json:"nodes"`
	// Relationships is a list of relationships in the graph
	Relationships []Relationship `json:"relationships"`
	// Source is the document from which the graph information was derived
	Source schema.Document `json:"source"`
}

// NewNode creates a new Node with the given ID and type.
func NewNode(id, nodeType string) Node {
	return Node{
		ID:         id,
		Type:       nodeType,
		Properties: make(map[string]interface{}),
	}
}

// SetProperty sets a property on the node.
func (n *Node) SetProperty(key string, value interface{}) {
	if n.Properties == nil {
		n.Properties = make(map[string]interface{})
	}
	n.Properties[key] = value
}

// GetProperty gets a property from the node.
func (n *Node) GetProperty(key string) (interface{}, bool) {
	if n.Properties == nil {
		return nil, false
	}
	val, ok := n.Properties[key]
	return val, ok
}

// HasProperty checks if the node has a specific property.
func (n *Node) HasProperty(key string) bool {
	_, ok := n.GetProperty(key)
	return ok
}

// RemoveProperty removes a property from the node.
func (n *Node) RemoveProperty(key string) bool {
	if n.Properties == nil {
		return false
	}
	if _, exists := n.Properties[key]; exists {
		delete(n.Properties, key)
		return true
	}
	return false
}

// GetPropertyKeys returns all property keys for the node.
func (n *Node) GetPropertyKeys() []string {
	if n.Properties == nil {
		return []string{}
	}
	keys := make([]string, 0, len(n.Properties))
	for key := range n.Properties {
		keys = append(keys, key)
	}
	return keys
}

// Clone creates a deep copy of the node.
func (n *Node) Clone() Node {
	clone := Node{
		ID:         n.ID,
		Type:       n.Type,
		Properties: make(map[string]interface{}),
	}
	for k, v := range n.Properties {
		clone.Properties[k] = v
	}
	return clone
}

// NewRelationship creates a relationship betweeen source and target nodes
func NewRelationship(source, target Node, relType string) Relationship {
	return Relationship{
		Source:     source,
		Target:     target,
		Type:       relType,
		Properties: make(map[string]interface{}),
	}
}

// SetProperty sets a property on the relationship.
func (r *Relationship) SetProperty(key string, value interface{}) {
	if r.Properties == nil {
		r.Properties = make(map[string]interface{})
	}
	r.Properties[key] = value
}

// GetProperty gets a property from the relationship
func (r *Relationship) GetProperty(key string) (interface{}, bool) {
	if r.Properties == nil {
		return nil, false
	}
	val, ok := r.Properties[key]
	return val, ok
}

// HasProperty checks if the relationship has a specific property.
func (r *Relationship) HasProperty(key string) bool {
	_, ok := r.GetProperty(key)
	return ok
}

// RemoveProperty removes a property from the relationship.
func (r *Relationship) RemoveProperty(key string) bool {
	if r.Properties == nil {
		return false
	}
	if _, exists := r.Properties[key]; exists {
		delete(r.Properties, key)
		return true
	}
	return false
}

// GetPropertyKeys returns all property keys for the relationship.
func (r *Relationship) GetPropertyKeys() []string {
	if r.Properties == nil {
		return []string{}
	}
	keys := make([]string, 0, len(r.Properties))
	for key := range r.Properties {
		keys = append(keys, key)
	}
	return keys
}

// Clone creates a deep copy of the relationship.
func (r *Relationship) Clone() Relationship {
	clone := Relationship{
		Source:     r.Source.Clone(),
		Target:     r.Target.Clone(),
		Type:       r.Type,
		Properties: make(map[string]interface{}),
	}
	for k, v := range r.Properties {
		clone.Properties[k] = v
	}
	return clone
}

// GetIdentifier returns a RelationshipIdentifier for this relationship.
func (r *Relationship) GetIdentifier() RelationshipIdentifier {
	return RelationshipIdentifier{
		SourceID: r.Source.ID,
		TargetID: r.Target.ID,
		Type:     r.Type,
	}
}

// NewGraphDocument creates a new GraphDocument with the given source document
func NewGraphDocument(source schema.Document) GraphDocument {
	return GraphDocument{
		Nodes:         make([]Node, 0),
		Relationships: make([]Relationship, 0),
		Source:        source,
	}
}

// AddNode adds a node to the GraphDocument
func (gd *GraphDocument) AddNode(node Node) {
	gd.Nodes = append(gd.Nodes, node)
}

// AddRelationship adds a relationship to the GraphDocument
func (gd *GraphDocument) AddRelationship(rel Relationship) {
	gd.Relationships = append(gd.Relationships, rel)
}

// RemoveNode removes a node from the GraphDocument by ID
func (gd *GraphDocument) RemoveNode(nodeID string) bool {
	for i, node := range gd.Nodes {
		if node.ID == nodeID {
			// Remove node from slice
			gd.Nodes = append(gd.Nodes[:i], gd.Nodes[i+1:]...)

			// Remove all relationships involving this node
			gd.removeRelationshipsByNodeID(nodeID)
			return true
		}
	}
	return false
}

// RemoveNodes removes multiple nodes from the GraphDocument by IDs
func (gd *GraphDocument) RemoveNodes(nodeIDs []string) int {
	removed := 0
	for _, nodeID := range nodeIDs {
		if gd.RemoveNode(nodeID) {
			removed++
		}
	}
	return removed
}

// RemoveRelationship removes a relationship from the GraphDocument
func (gd *GraphDocument) RemoveRelationship(sourceID, targetID, relType string) bool {
	for i, rel := range gd.Relationships {
		if rel.Source.ID == sourceID && rel.Target.ID == targetID && rel.Type == relType {
			// Remove relationship from slice
			gd.Relationships = append(gd.Relationships[:i], gd.Relationships[i+1:]...)
			return true
		}
	}
	return false
}

// RemoveRelationships removes multiple relationships from the GraphDocument
func (gd *GraphDocument) RemoveRelationships(identifiers []RelationshipIdentifier) int {
	removed := 0
	for _, id := range identifiers {
		if gd.RemoveRelationship(id.SourceID, id.TargetID, id.Type) {
			removed++
		}
	}
	return removed
}

// removeRelationshipsByNodeID removes all relationships involving a specific node
func (gd *GraphDocument) removeRelationshipsByNodeID(nodeID string) {
	filtered := make([]Relationship, 0, len(gd.Relationships))
	for _, rel := range gd.Relationships {
		if rel.Source.ID != nodeID && rel.Target.ID != nodeID {
			filtered = append(filtered, rel)
		}
	}
	gd.Relationships = filtered
}

// FindNode finds a node by ID
func (gd *GraphDocument) FindNode(nodeID string) *Node {
	for i, node := range gd.Nodes {
		if node.ID == nodeID {
			return &gd.Nodes[i]
		}
	}
	return nil
}

// FindNodesByType finds all nodes of a specific type
func (gd *GraphDocument) FindNodesByType(nodeType string) []Node {
	var nodes []Node
	for _, node := range gd.Nodes {
		if node.Type == nodeType {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// FindRelationship finds a relationship by source, target, and type
func (gd *GraphDocument) FindRelationship(sourceID, targetID, relType string) *Relationship {
	for i, rel := range gd.Relationships {
		if rel.Source.ID == sourceID && rel.Target.ID == targetID && rel.Type == relType {
			return &gd.Relationships[i]
		}
	}
	return nil
}

// FindRelationshipsByType finds all relationships of a specific type
func (gd *GraphDocument) FindRelationshipsByType(relType string) []Relationship {
	var relationships []Relationship
	for _, rel := range gd.Relationships {
		if rel.Type == relType {
			relationships = append(relationships, rel)
		}
	}
	return relationships
}

// FindRelationshipsByNode finds all relationships involving a specific node
func (gd *GraphDocument) FindRelationshipsByNode(nodeID string) []Relationship {
	var relationships []Relationship
	for _, rel := range gd.Relationships {
		if rel.Source.ID == nodeID || rel.Target.ID == nodeID {
			relationships = append(relationships, rel)
		}
	}
	return relationships
}

// UpdateNode updates an existing node's properties
func (gd *GraphDocument) UpdateNode(nodeID string, properties map[string]interface{}) bool {
	node := gd.FindNode(nodeID)
	if node == nil {
		return false
	}

	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}

	for key, value := range properties {
		node.Properties[key] = value
	}
	return true
}

// UpdateRelationship updates an existing relationship's properties
func (gd *GraphDocument) UpdateRelationship(sourceID, targetID, relType string, properties map[string]interface{}) bool {
	rel := gd.FindRelationship(sourceID, targetID, relType)
	if rel == nil {
		return false
	}

	if rel.Properties == nil {
		rel.Properties = make(map[string]interface{})
	}

	for key, value := range properties {
		rel.Properties[key] = value
	}
	return true
}

// NodeExists checks if a node exists in the GraphDocument
func (gd *GraphDocument) NodeExists(nodeID string) bool {
	return gd.FindNode(nodeID) != nil
}

// RelationshipExists checks if a relationship exists in the GraphDocument
func (gd *GraphDocument) RelationshipExists(sourceID, targetID, relType string) bool {
	return gd.FindRelationship(sourceID, targetID, relType) != nil
}

// GetNodeCount returns the number of nodes in the GraphDocument
func (gd *GraphDocument) GetNodeCount() int {
	return len(gd.Nodes)
}

// GetRelationshipCount returns the number of relationships in the GraphDocument
func (gd *GraphDocument) GetRelationshipCount() int {
	return len(gd.Relationships)
}

// GetNodeTypes returns all unique node types in the GraphDocument
func (gd *GraphDocument) GetNodeTypes() []string {
	types := make(map[string]bool)
	for _, node := range gd.Nodes {
		types[node.Type] = true
	}

	result := make([]string, 0, len(types))
	for nodeType := range types {
		result = append(result, nodeType)
	}
	return result
}

// GetRelationshipTypes returns all unique relationship types in the GraphDocument
func (gd *GraphDocument) GetRelationshipTypes() []string {
	types := make(map[string]bool)
	for _, rel := range gd.Relationships {
		types[rel.Type] = true
	}

	result := make([]string, 0, len(types))
	for relType := range types {
		result = append(result, relType)
	}
	return result
}

// Merge merges another GraphDocument into this one
func (gd *GraphDocument) Merge(other *GraphDocument) {
	// Add nodes that don't already exist
	for _, node := range other.Nodes {
		if !gd.NodeExists(node.ID) {
			gd.AddNode(node)
		}
	}

	// Add relationships that don't already exist
	for _, rel := range other.Relationships {
		if !gd.RelationshipExists(rel.Source.ID, rel.Target.ID, rel.Type) {
			gd.AddRelationship(rel)
		}
	}
}

// Clone creates a deep copy of the GraphDocument
func (gd *GraphDocument) Clone() *GraphDocument {
	clone := NewGraphDocument(gd.Source)

	// Copy nodes
	for _, node := range gd.Nodes {
		newNode := Node{
			ID:         node.ID,
			Type:       node.Type,
			Properties: make(map[string]interface{}),
		}
		for k, v := range node.Properties {
			newNode.Properties[k] = v
		}
		clone.AddNode(newNode)
	}

	// Copy relationships
	for _, rel := range gd.Relationships {
		newRel := Relationship{
			Source:     rel.Source,
			Target:     rel.Target,
			Type:       rel.Type,
			Properties: make(map[string]interface{}),
		}
		for k, v := range rel.Properties {
			newRel.Properties[k] = v
		}
		clone.AddRelationship(newRel)
	}

	return &clone
}

// ToJSON converts the GraphDocument to a JSON representation
func (gd *GraphDocument) ToJSON() ([]byte, error) {
	return json.Marshal(gd)
}

// FromJSON creates a GraphDocument from JSON
func FromJSON(data []byte) (*GraphDocument, error) {
	var gd GraphDocument
	err := json.Unmarshal(data, &gd)
	if err != nil {
		return nil, err
	}
	return &gd, nil
}
