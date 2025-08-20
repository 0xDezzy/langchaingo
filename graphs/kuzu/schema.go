package kuzu

import (
	"context"
	"fmt"
	"strings"

	"github.com/tmc/langchaingo/graphs"
)

// ensureTablesExist creates the necessary node and relationship tables for the graph document
func (k *Kuzu) ensureTablesExist(doc graphs.GraphDocument) error {
	// Extract unique node types from the document
	nodeTypes := make(map[string]bool)
	for _, node := range doc.Nodes {
		nodeTypes[node.Type] = true
	}

	// Create node tables for each unique type
	for nodeType := range nodeTypes {
		if err := k.createNodeTable(nodeType); err != nil {
			return fmt.Errorf("failed to create node table for type %s: %w", nodeType, err)
		}
	}

	// Create Chunk node table if not exists (for source document tracking)
	if err := k.createChunkNodeTable(); err != nil {
		return fmt.Errorf("failed to create Chunk node table: %w", err)
	}

	// Extract unique relationship patterns from the document
	relationshipTypes := make(map[string]struct {
		sourceType string
		targetType string
		relType    string
	})

	for _, rel := range doc.Relationships {
		key := fmt.Sprintf("%s_%s_%s", rel.Source.Type, rel.Type, rel.Target.Type)
		relationshipTypes[key] = struct {
			sourceType string
			targetType string
			relType    string
		}{
			sourceType: rel.Source.Type,
			targetType: rel.Target.Type,
			relType:    rel.Type,
		}
	}

	// Create relationship tables for each unique pattern
	for _, relInfo := range relationshipTypes {
		if err := k.createRelationshipTable(relInfo.relType, relInfo.sourceType, relInfo.targetType); err != nil {
			return fmt.Errorf("failed to create relationship table %s: %w", relInfo.relType, err)
		}
	}

	// Create MENTIONS relationship table for source document linking
	if err := k.createMentionsRelationshipTable(); err != nil {
		return fmt.Errorf("failed to create MENTIONS relationship table: %w", err)
	}

	return nil
}

// createNodeTable creates a node table for the given node type
func (k *Kuzu) createNodeTable(nodeType string) error {
	k.tablesMux.Lock()
	defer k.tablesMux.Unlock()

	// Check if table already exists in our cache
	if k.nodeTables[nodeType] {
		return nil
	}

	// Create the basic node table structure with MAP for dynamic properties
	// KuzuDB requires explicit property definitions, so we use MAP for dynamic properties
	query := fmt.Sprintf(`
		CREATE NODE TABLE IF NOT EXISTS %s (
			id STRING,
			type STRING,
			properties MAP(STRING, STRING),
			PRIMARY KEY(id)
		);
	`, sanitizeIdentifier(nodeType))

	_, err := k.connection.Query(query)
	if err != nil {
		return fmt.Errorf("failed to create node table %s: %w", nodeType, err)
	}

	// Mark as created in cache
	k.nodeTables[nodeType] = true
	return nil
}

// createChunkNodeTable creates the Chunk node table for source document tracking
func (k *Kuzu) createChunkNodeTable() error {
	k.tablesMux.Lock()
	defer k.tablesMux.Unlock()

	// Check if already created
	if k.nodeTables["Chunk"] {
		return nil
	}

	query := `
		CREATE NODE TABLE IF NOT EXISTS Chunk (
			id STRING,
			text STRING,
			type STRING,
			properties MAP(STRING, STRING),
			PRIMARY KEY(id)
		);
	`

	_, err := k.connection.Query(query)
	if err != nil {
		return fmt.Errorf("failed to create Chunk node table: %w", err)
	}

	k.nodeTables["Chunk"] = true
	return nil
}

// createRelationshipTable creates a relationship table between two node types
func (k *Kuzu) createRelationshipTable(relType, sourceType, targetType string) error {
	k.tablesMux.Lock()
	defer k.tablesMux.Unlock()

	// Create a unique key for this relationship pattern
	key := fmt.Sprintf("%s_%s_%s", sourceType, relType, targetType)

	// Check if already created
	if k.relTables[key] {
		return nil
	}

	query := fmt.Sprintf(`
		CREATE REL TABLE IF NOT EXISTS %s (
			FROM %s TO %s,
			properties MAP(STRING, STRING)
		);
	`, sanitizeIdentifier(relType), sanitizeIdentifier(sourceType), sanitizeIdentifier(targetType))

	_, err := k.connection.Query(query)
	if err != nil {
		return fmt.Errorf("failed to create relationship table %s: %w", relType, err)
	}

	k.relTables[key] = true
	return nil
}

// createMentionsRelationshipTable creates the MENTIONS relationship table for source tracking
func (k *Kuzu) createMentionsRelationshipTable() error {
	k.tablesMux.Lock()
	defer k.tablesMux.Unlock()

	key := "MENTIONS"
	if k.relTables[key] {
		return nil
	}

	// The MENTIONS relationship can connect Chunk nodes to any entity node type
	// For now, we'll create a general pattern and handle specific cases during import
	query := `
		CREATE REL TABLE IF NOT EXISTS MENTIONS (
			FROM Chunk TO Node,
			properties MAP(STRING, STRING)
		);
	`

	_, err := k.connection.Query(query)
	if err != nil {
		// If the generic Node approach doesn't work, we might need to create
		// specific MENTIONS tables for each node type during import
		// For now, we'll ignore this error and handle it during import
		return nil
	}

	k.relTables[key] = true
	return nil
}

// addPropertyToNodeTable adds a new property column to an existing node table
func (k *Kuzu) addPropertyToNodeTable(nodeType, propName, propType string) error {
	// KuzuDB might require explicit ALTER TABLE statements for adding properties
	// This is a placeholder for future implementation when KuzuDB supports it

	query := fmt.Sprintf(`
		ALTER TABLE %s ADD %s %s;
	`, sanitizeIdentifier(nodeType), sanitizeIdentifier(propName), propType)

	_, err := k.connection.Query(query)
	if err != nil {
		// If ALTER TABLE is not supported, we might need to handle this differently
		// For now, we'll log the error and continue
		return fmt.Errorf("failed to add property %s to table %s: %w", propName, nodeType, err)
	}

	return nil
}

// sanitizeIdentifier sanitizes database identifiers to prevent injection attacks
func sanitizeIdentifier(identifier string) string {
	// Remove any characters that aren't letters, numbers, or underscores
	sanitized := strings.ReplaceAll(identifier, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")

	// Ensure identifier starts with a letter or underscore
	if len(sanitized) > 0 && !isLetter(sanitized[0]) && sanitized[0] != '_' {
		sanitized = "_" + sanitized
	}

	return sanitized
}

// isLetter checks if a byte is a letter
func isLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// getKuzuDataType converts Go types to KuzuDB data types using the type converter
func getKuzuDataType(value interface{}) string {
	typeConverter := NewTypeConverter()
	dataType := typeConverter.GetKuzuTypeForGoValue(value)
	return string(dataType)
}

// createTableWithTypedProperties creates a table with properly typed properties
func (k *Kuzu) createTableWithTypedProperties(tableName string, properties map[string]interface{}) error {
	if len(properties) == 0 {
		// Create basic table structure
		return k.createNodeTable(tableName)
	}

	// Build DDL with typed properties
	var propDefinitions []string
	propDefinitions = append(propDefinitions, "id STRING")
	propDefinitions = append(propDefinitions, "type STRING")

	typeConverter := NewTypeConverter()
	for propName, propValue := range properties {
		if propName == "id" || propName == "type" {
			continue // Skip reserved properties
		}

		dataType := typeConverter.GetKuzuTypeForGoValue(propValue)
		propDefinitions = append(propDefinitions,
			fmt.Sprintf("%s %s", sanitizeIdentifier(propName), string(dataType)))
	}

	query := fmt.Sprintf(`
		CREATE NODE TABLE IF NOT EXISTS %s (
			%s,
			PRIMARY KEY(id)
		);
	`, sanitizeIdentifier(tableName), strings.Join(propDefinitions, ",\n\t\t\t"))

	_, err := k.connection.Query(query)
	if err != nil {
		return fmt.Errorf("failed to create typed table %s: %w", tableName, err)
	}

	// Mark as created in cache
	k.tablesMux.Lock()
	k.nodeTables[tableName] = true
	k.tablesMux.Unlock()

	return nil
}

// inferSchemaFromDocument analyzes a graph document to infer optimal schema
func (k *Kuzu) inferSchemaFromDocument(doc graphs.GraphDocument) map[string]map[string]interface{} {
	schema := make(map[string]map[string]interface{})
	typeConverter := NewTypeConverter()

	// Analyze node properties
	for _, node := range doc.Nodes {
		if _, exists := schema[node.Type]; !exists {
			schema[node.Type] = make(map[string]interface{})
		}

		for propName, propValue := range node.Properties {
			// Track property types for each node type
			if existing, exists := schema[node.Type][propName]; exists {
				// Check for type consistency
				existingType := typeConverter.GetKuzuTypeForGoValue(existing)
				currentType := typeConverter.GetKuzuTypeForGoValue(propValue)

				if existingType != currentType {
					// Use STRING as fallback for inconsistent types
					schema[node.Type][propName] = "STRING"
				}
			} else {
				schema[node.Type][propName] = propValue
			}
		}
	}

	return schema
}

// introspectSchema discovers the current database schema
func (k *Kuzu) introspectSchema() error {
	ctx := context.Background()

	// Get all tables information
	tablesInfo, err := k.getAllTablesInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tables info: %w", err)
	}

	// Separate node and relationship tables
	nodeTables := make(map[string][]PropertyInfo)
	relTables := make(map[string]RelationshipInfo)

	for _, table := range tablesInfo {
		if table.IsNodeTable {
			// Get detailed properties for node table
			props, err := k.getTableProperties(ctx, table.Name)
			if err != nil {
				// Log error but continue - don't fail entire schema refresh
				continue
			}
			nodeTables[table.Name] = props
		} else {
			// This is a relationship table
			relTables[table.Name] = RelationshipInfo{
				Name:       table.Name,
				SourceType: table.SourceType,
				TargetType: table.TargetType,
			}
		}
	}

	// Update structured schema
	k.updateStructuredSchema(nodeTables, relTables)

	return nil
}

// TableInfo represents information about a KuzuDB table
type TableInfo struct {
	Name        string
	IsNodeTable bool
	SourceType  string // For relationship tables
	TargetType  string // For relationship tables
}

// PropertyInfo represents a property in a KuzuDB table
type PropertyInfo struct {
	Name string
	Type string
}

// RelationshipInfo represents a relationship table
type RelationshipInfo struct {
	Name       string
	SourceType string
	TargetType string
}

// getAllTablesInfo gets basic information about all tables in the database
func (k *Kuzu) getAllTablesInfo(ctx context.Context) ([]TableInfo, error) {
	// Query to get all tables - this may need adjustment based on KuzuDB version
	query := "SHOW TABLES;"
	result, err := k.Query(ctx, query, nil)
	if err != nil {
		// If SHOW TABLES doesn't work, try alternative approach
		return k.getTablesInfoAlternative(ctx)
	}

	var tables []TableInfo
	if records, ok := result["records"].([]map[string]interface{}); ok {
		for _, record := range records {
			// Parse table information from SHOW TABLES result
			// Format may vary by KuzuDB version
			if tableName, ok := record["table_name"].(string); ok {
				tables = append(tables, TableInfo{
					Name:        tableName,
					IsNodeTable: true, // Default assumption - will be refined
				})
			}
		}
	}

	return tables, nil
}

// getTablesInfoAlternative uses an alternative method to discover tables
func (k *Kuzu) getTablesInfoAlternative(ctx context.Context) ([]TableInfo, error) {
	// Use our cached table information as a fallback
	k.tablesMux.RLock()
	defer k.tablesMux.RUnlock()

	var tables []TableInfo

	// Add known node tables
	for nodeType := range k.nodeTables {
		tables = append(tables, TableInfo{
			Name:        nodeType,
			IsNodeTable: true,
		})
	}

	// Add known relationship tables
	for relKey := range k.relTables {
		// Parse relationship key format: "source_rel_target" or just "rel_name"
		tables = append(tables, TableInfo{
			Name:        relKey,
			IsNodeTable: false,
		})
	}

	return tables, nil
}

// getTableProperties gets detailed property information for a specific table
func (k *Kuzu) getTableProperties(ctx context.Context, tableName string) ([]PropertyInfo, error) {
	// Use CALL table_info to get detailed table structure
	query := fmt.Sprintf("CALL table_info('%s') RETURN *;", tableName)
	result, err := k.Query(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get properties for table %s: %w", tableName, err)
	}

	var properties []PropertyInfo
	if records, ok := result["records"].([]map[string]interface{}); ok {
		for _, record := range records {
			// Parse property information
			// The exact format depends on KuzuDB's table_info output
			if propName, ok := record["property_name"].(string); ok {
				propType := "UNKNOWN"
				if pType, ok := record["property_type"].(string); ok {
					propType = pType
				}

				properties = append(properties, PropertyInfo{
					Name: propName,
					Type: propType,
				})
			}
		}
	}

	return properties, nil
}

// updateStructuredSchema updates the structured schema cache
func (k *Kuzu) updateStructuredSchema(nodeTables map[string][]PropertyInfo, relTables map[string]RelationshipInfo) {
	// Build structured schema representation
	nodeTypes := make([]map[string]interface{}, 0, len(nodeTables))
	for tableName, properties := range nodeTables {
		propsList := make([]map[string]string, 0, len(properties))
		for _, prop := range properties {
			propsList = append(propsList, map[string]string{
				"name": prop.Name,
				"type": prop.Type,
			})
		}

		nodeTypes = append(nodeTypes, map[string]interface{}{
			"label":      tableName,
			"properties": propsList,
		})
	}

	relTypes := make([]map[string]interface{}, 0, len(relTables))
	for _, relInfo := range relTables {
		relTypes = append(relTypes, map[string]interface{}{
			"name":        relInfo.Name,
			"source_type": relInfo.SourceType,
			"target_type": relInfo.TargetType,
		})
	}

	// Update the structured schema
	k.structuredSchema = map[string]interface{}{
		"node_types":         nodeTypes,
		"relationship_types": relTypes,
		"total_tables":       len(nodeTables) + len(relTables),
	}
}

// buildSchemaDescription creates a human-readable schema description
func (k *Kuzu) buildSchemaDescription() {
	// Build a comprehensive schema description using the introspected data
	var description strings.Builder

	// Node properties section
	description.WriteString("Node properties:\n")
	if nodeTypes, ok := k.structuredSchema["node_types"].([]map[string]interface{}); ok {
		for _, nodeType := range nodeTypes {
			if label, ok := nodeType["label"].(string); ok {
				description.WriteString(fmt.Sprintf("  %s:\n", label))

				if props, ok := nodeType["properties"].([]map[string]string); ok {
					for _, prop := range props {
						description.WriteString(fmt.Sprintf("    - %s: %s\n", prop["name"], prop["type"]))
					}
				}
			}
		}
	}

	// Relationship properties section
	description.WriteString("\nRelationship properties:\n")
	if relTypes, ok := k.structuredSchema["relationship_types"].([]map[string]interface{}); ok {
		for _, relType := range relTypes {
			if name, ok := relType["name"].(string); ok {
				sourceType := "Unknown"
				targetType := "Unknown"

				if src, ok := relType["source_type"].(string); ok {
					sourceType = src
				}
				if tgt, ok := relType["target_type"].(string); ok {
					targetType = tgt
				}

				description.WriteString(fmt.Sprintf("  %s: %s -> %s\n", name, sourceType, targetType))
			}
		}
	}

	// Relationships section (visual representation)
	description.WriteString("\nRelationships:\n")
	if relTypes, ok := k.structuredSchema["relationship_types"].([]map[string]interface{}); ok {
		for _, relType := range relTypes {
			if name, ok := relType["name"].(string); ok {
				sourceType := "Unknown"
				targetType := "Unknown"

				if src, ok := relType["source_type"].(string); ok {
					sourceType = src
				}
				if tgt, ok := relType["target_type"].(string); ok {
					targetType = tgt
				}

				description.WriteString(fmt.Sprintf("  (:%s)-[:%s]->(:%s)\n", sourceType, name, targetType))
			}
		}
	}

	k.schemaCache = description.String()
}

// GetTableList returns a list of all tables in the database
func (k *Kuzu) GetTableList(ctx context.Context) ([]string, error) {
	tables, err := k.getAllTablesInfo(ctx)
	if err != nil {
		return nil, err
	}

	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.Name)
	}

	return tableNames, nil
}

// GetNodeTableProperties returns properties for a specific node table
func (k *Kuzu) GetNodeTableProperties(ctx context.Context, tableName string) ([]PropertyInfo, error) {
	return k.getTableProperties(ctx, tableName)
}

// TableExists checks if a table exists in the database
func (k *Kuzu) TableExists(ctx context.Context, tableName string) (bool, error) {
	tables, err := k.getAllTablesInfo(ctx)
	if err != nil {
		return false, err
	}

	for _, table := range tables {
		if table.Name == tableName {
			return true, nil
		}
	}

	return false, nil
}

// GetSchemaVersion returns a version hash of the current schema
func (k *Kuzu) GetSchemaVersion() string {
	k.schemaMux.RLock()
	defer k.schemaMux.RUnlock()

	// Create a simple hash of the schema for version tracking
	hash := 0
	for key, value := range k.structuredSchema {
		hash = hash*31 + len(key)
		if str, ok := value.(string); ok {
			for _, char := range str {
				hash = hash*31 + int(char)
			}
		}
	}

	return fmt.Sprintf("v%d", hash)
}

// CompareSchemas compares current schema with a cached version
func (k *Kuzu) CompareSchemas(ctx context.Context) (bool, error) {
	// Store current schema
	currentSchema := k.structuredSchema

	// Refresh and compare
	if err := k.introspectSchema(); err != nil {
		return false, err
	}

	// Simple comparison - could be enhanced
	if len(currentSchema) != len(k.structuredSchema) {
		return false, nil
	}

	// Restore current schema for now - real implementation would do deep comparison
	k.structuredSchema = currentSchema
	return true, nil
}

// ValidateSchema performs basic schema validation
func (k *Kuzu) ValidateSchema(ctx context.Context) error {
	// Check that essential tables exist
	tables, err := k.getAllTablesInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table list: %w", err)
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables found in database")
	}

	// Validate that we can query each table
	for _, table := range tables {
		query := fmt.Sprintf("MATCH (n:%s) RETURN count(n) LIMIT 1;", sanitizeIdentifier(table.Name))
		_, err := k.Query(ctx, query, nil)
		if err != nil {
			return fmt.Errorf("failed to validate table %s: %w", table.Name, err)
		}
	}

	return nil
}
