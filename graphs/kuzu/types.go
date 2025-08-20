package kuzu

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// KuzuDataType represents KuzuDB data types
type KuzuDataType string

const (
	// Basic types
	KuzuINT64     KuzuDataType = "INT64"
	KuzuINT32     KuzuDataType = "INT32"
	KuzuINT16     KuzuDataType = "INT16"
	KuzuINT8      KuzuDataType = "INT8"
	KuzuUINT64    KuzuDataType = "UINT64"
	KuzuUINT32    KuzuDataType = "UINT32"
	KuzuUINT16    KuzuDataType = "UINT16"
	KuzuUINT8     KuzuDataType = "UINT8"
	KuzuDOUBLE    KuzuDataType = "DOUBLE"
	KuzuFLOAT     KuzuDataType = "FLOAT"
	KuzuSTRING    KuzuDataType = "STRING"
	KuzuBOOL      KuzuDataType = "BOOL"
	KuzuDATE      KuzuDataType = "DATE"
	KuzuTIMESTAMP KuzuDataType = "TIMESTAMP"
	KuzuINTERVAL  KuzuDataType = "INTERVAL"

	// Complex types
	KuzuLIST   KuzuDataType = "LIST"
	KuzuSTRUCT KuzuDataType = "STRUCT"
	KuzuMAP    KuzuDataType = "MAP"
	KuzuUNION  KuzuDataType = "UNION"

	// Special types
	KuzuNODE         KuzuDataType = "NODE"
	KuzuRELATIONSHIP KuzuDataType = "RELATIONSHIP"
	KuzuPATH         KuzuDataType = "PATH"
)

// TypeConverter handles conversion between Go types and KuzuDB types
type TypeConverter struct {
	// Configuration for type conversion
	StrictMode       bool   // Whether to enforce strict type checking
	DateFormat       string // Format for date parsing
	TimestampFormat  string // Format for timestamp parsing
	DecimalPrecision int    // Precision for decimal numbers
}

// NewTypeConverter creates a new type converter with default settings
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{
		StrictMode:       false,
		DateFormat:       "2006-01-02",
		TimestampFormat:  time.RFC3339,
		DecimalPrecision: 15,
	}
}

// ConvertGoValueToKuzu converts a Go value to a KuzuDB-compatible value
func (tc *TypeConverter) ConvertGoValueToKuzu(value interface{}) (interface{}, KuzuDataType, error) {
	if value == nil {
		return nil, KuzuSTRING, nil
	}

	switch v := value.(type) {
	case string:
		return tc.convertString(v)
	case bool:
		return v, KuzuBOOL, nil
	case int:
		return int64(v), KuzuINT64, nil
	case int8:
		return v, KuzuINT8, nil
	case int16:
		return v, KuzuINT16, nil
	case int32:
		return v, KuzuINT32, nil
	case int64:
		return v, KuzuINT64, nil
	case uint:
		return uint64(v), KuzuUINT64, nil
	case uint8:
		return v, KuzuUINT8, nil
	case uint16:
		return v, KuzuUINT16, nil
	case uint32:
		return v, KuzuUINT32, nil
	case uint64:
		return v, KuzuUINT64, nil
	case float32:
		return v, KuzuFLOAT, nil
	case float64:
		return v, KuzuDOUBLE, nil
	case time.Time:
		return tc.convertTime(v)
	case []interface{}:
		return tc.convertSlice(v)
	case map[string]interface{}:
		return tc.convertMap(v)
	default:
		return tc.convertReflect(value)
	}
}

// convertString handles string conversion with type detection
func (tc *TypeConverter) convertString(s string) (interface{}, KuzuDataType, error) {
	// Try to detect if it's a special type encoded as string
	if tc.isDateString(s) {
		if date, err := time.Parse(tc.DateFormat, s); err == nil {
			return date.Format(tc.DateFormat), KuzuDATE, nil
		}
	}

	if tc.isTimestampString(s) {
		if timestamp, err := time.Parse(tc.TimestampFormat, s); err == nil {
			return timestamp.Format(tc.TimestampFormat), KuzuTIMESTAMP, nil
		}
	}

	// Try to detect JSON structures
	if tc.isJSONString(s) {
		var obj interface{}
		if err := json.Unmarshal([]byte(s), &obj); err == nil {
			return tc.ConvertGoValueToKuzu(obj)
		}
	}

	return s, KuzuSTRING, nil
}

// convertTime handles time.Time conversion
func (tc *TypeConverter) convertTime(t time.Time) (interface{}, KuzuDataType, error) {
	// Check if it's a date-only value (time components are zero)
	if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
		return t.Format(tc.DateFormat), KuzuDATE, nil
	}
	return t.Format(tc.TimestampFormat), KuzuTIMESTAMP, nil
}

// convertSlice handles slice/array conversion
func (tc *TypeConverter) convertSlice(slice []interface{}) (interface{}, KuzuDataType, error) {
	if len(slice) == 0 {
		return slice, KuzuLIST, nil
	}

	// Convert all elements and determine uniform type
	convertedSlice := make([]interface{}, len(slice))
	var elementType KuzuDataType

	for i, item := range slice {
		converted, itemType, err := tc.ConvertGoValueToKuzu(item)
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert slice element %d: %w", i, err)
		}

		convertedSlice[i] = converted

		// Set or validate element type consistency
		if i == 0 {
			elementType = itemType
		} else if elementType != itemType && tc.StrictMode {
			return nil, "", fmt.Errorf("inconsistent types in slice: %s vs %s", elementType, itemType)
		}
	}

	return convertedSlice, KuzuLIST, nil
}

// convertMap handles map conversion to STRUCT
func (tc *TypeConverter) convertMap(m map[string]interface{}) (interface{}, KuzuDataType, error) {
	convertedMap := make(map[string]interface{})

	for key, value := range m {
		converted, _, err := tc.ConvertGoValueToKuzu(value)
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert map value for key '%s': %w", key, err)
		}
		convertedMap[key] = converted
	}

	return convertedMap, KuzuSTRUCT, nil
}

// convertReflect handles conversion using reflection for custom types
func (tc *TypeConverter) convertReflect(value interface{}) (interface{}, KuzuDataType, error) {
	v := reflect.ValueOf(value)
	t := reflect.TypeOf(value)

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return nil, KuzuSTRING, nil
		}
		return tc.ConvertGoValueToKuzu(v.Elem().Interface())

	case reflect.Struct:
		// Convert struct to map
		structMap := make(map[string]interface{})
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			fieldValue := v.Field(i)

			if !fieldValue.CanInterface() {
				continue // Skip unexported fields
			}

			// Use JSON tag if available, otherwise field name
			fieldName := field.Name
			if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
				if comma := strings.Index(jsonTag, ","); comma >= 0 {
					fieldName = jsonTag[:comma]
				} else {
					fieldName = jsonTag
				}
			}

			converted, _, err := tc.ConvertGoValueToKuzu(fieldValue.Interface())
			if err != nil {
				return nil, "", fmt.Errorf("failed to convert struct field '%s': %w", fieldName, err)
			}
			structMap[fieldName] = converted
		}
		return structMap, KuzuSTRUCT, nil

	case reflect.Slice, reflect.Array:
		// Convert to []interface{}
		slice := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			slice[i] = v.Index(i).Interface()
		}
		return tc.convertSlice(slice)

	default:
		// Fallback: convert to string
		return fmt.Sprintf("%v", value), KuzuSTRING, nil
	}
}

// ConvertKuzuValueToGo converts a KuzuDB value back to a Go value
func (tc *TypeConverter) ConvertKuzuValueToGo(value interface{}, dataType KuzuDataType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch dataType {
	case KuzuSTRING:
		return tc.convertKuzuString(value)
	case KuzuBOOL:
		return tc.convertKuzuBool(value)
	case KuzuINT8, KuzuINT16, KuzuINT32, KuzuINT64:
		return tc.convertKuzuInt(value, dataType)
	case KuzuUINT8, KuzuUINT16, KuzuUINT32, KuzuUINT64:
		return tc.convertKuzuUint(value, dataType)
	case KuzuFLOAT, KuzuDOUBLE:
		return tc.convertKuzuFloat(value, dataType)
	case KuzuDATE:
		return tc.convertKuzuDate(value)
	case KuzuTIMESTAMP:
		return tc.convertKuzuTimestamp(value)
	case KuzuLIST:
		return tc.convertKuzuList(value)
	case KuzuSTRUCT:
		return tc.convertKuzuStruct(value)
	default:
		// Default to original value
		return value, nil
	}
}

// Helper methods for KuzuDB to Go conversion

func (tc *TypeConverter) convertKuzuString(value interface{}) (interface{}, error) {
	if str, ok := value.(string); ok {
		return str, nil
	}
	return fmt.Sprintf("%v", value), nil
}

func (tc *TypeConverter) convertKuzuBool(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func (tc *TypeConverter) convertKuzuInt(value interface{}, dataType KuzuDataType) (interface{}, error) {
	switch v := value.(type) {
	case int64:
		switch dataType {
		case KuzuINT8:
			return int8(v), nil
		case KuzuINT16:
			return int16(v), nil
		case KuzuINT32:
			return int32(v), nil
		default:
			return v, nil
		}
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return nil, fmt.Errorf("cannot convert %T to int", value)
	}
}

func (tc *TypeConverter) convertKuzuUint(value interface{}, dataType KuzuDataType) (interface{}, error) {
	switch v := value.(type) {
	case uint64:
		switch dataType {
		case KuzuUINT8:
			return uint8(v), nil
		case KuzuUINT16:
			return uint16(v), nil
		case KuzuUINT32:
			return uint32(v), nil
		default:
			return v, nil
		}
	case int64:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return nil, fmt.Errorf("cannot convert %T to uint", value)
	}
}

func (tc *TypeConverter) convertKuzuFloat(value interface{}, dataType KuzuDataType) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		if dataType == KuzuFLOAT {
			return float32(v), nil
		}
		return v, nil
	case int64:
		if dataType == KuzuFLOAT {
			return float32(v), nil
		}
		return float64(v), nil
	case string:
		if dataType == KuzuFLOAT {
			if f, err := strconv.ParseFloat(v, 32); err == nil {
				return float32(f), nil
			}
		}
		return strconv.ParseFloat(v, 64)
	default:
		return nil, fmt.Errorf("cannot convert %T to float", value)
	}
}

func (tc *TypeConverter) convertKuzuDate(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return time.Parse(tc.DateFormat, v)
	case time.Time:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to date", value)
	}
}

func (tc *TypeConverter) convertKuzuTimestamp(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return time.Parse(tc.TimestampFormat, v)
	case time.Time:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to timestamp", value)
	}
}

func (tc *TypeConverter) convertKuzuList(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []interface{}:
		return v, nil
	case string:
		// Try to parse as JSON array
		var arr []interface{}
		if err := json.Unmarshal([]byte(v), &arr); err == nil {
			return arr, nil
		}
		return nil, fmt.Errorf("cannot parse string as list: %s", v)
	default:
		return nil, fmt.Errorf("cannot convert %T to list", value)
	}
}

func (tc *TypeConverter) convertKuzuStruct(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		return v, nil
	case string:
		// Try to parse as JSON object
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(v), &obj); err == nil {
			return obj, nil
		}
		return nil, fmt.Errorf("cannot parse string as struct: %s", v)
	default:
		return nil, fmt.Errorf("cannot convert %T to struct", value)
	}
}

// Type detection helpers

func (tc *TypeConverter) isDateString(s string) bool {
	_, err := time.Parse(tc.DateFormat, s)
	return err == nil
}

func (tc *TypeConverter) isTimestampString(s string) bool {
	_, err := time.Parse(tc.TimestampFormat, s)
	return err == nil
}

func (tc *TypeConverter) isJSONString(s string) bool {
	return (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}")) ||
		(strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]"))
}

// GetKuzuTypeForGoValue determines the KuzuDB type for a Go value
func (tc *TypeConverter) GetKuzuTypeForGoValue(value interface{}) KuzuDataType {
	_, dataType, _ := tc.ConvertGoValueToKuzu(value)
	return dataType
}

// CreateKuzuTypedValue creates a typed value suitable for KuzuDB queries
func (tc *TypeConverter) CreateKuzuTypedValue(value interface{}) (map[string]interface{}, error) {
	converted, dataType, err := tc.ConvertGoValueToKuzu(value)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"value": converted,
		"type":  string(dataType),
	}, nil
}

// PropertyConverter handles property conversion for nodes and relationships
type PropertyConverter struct {
	TypeConverter *TypeConverter
}

// NewPropertyConverter creates a new property converter
func NewPropertyConverter() *PropertyConverter {
	return &PropertyConverter{
		TypeConverter: NewTypeConverter(),
	}
}

// ConvertProperties converts a map of properties for KuzuDB storage
func (pc *PropertyConverter) ConvertProperties(properties map[string]interface{}) (map[string]interface{}, error) {
	if properties == nil {
		return nil, nil
	}

	converted := make(map[string]interface{})
	for key, value := range properties {
		convertedValue, _, err := pc.TypeConverter.ConvertGoValueToKuzu(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert property '%s': %w", key, err)
		}
		converted[key] = convertedValue
	}

	return converted, nil
}

// SerializeComplexProperty serializes complex properties as JSON for storage
func (pc *PropertyConverter) SerializeComplexProperty(value interface{}) (string, error) {
	converted, dataType, err := pc.TypeConverter.ConvertGoValueToKuzu(value)
	if err != nil {
		return "", err
	}

	// If it's a complex type, serialize as JSON
	if dataType == KuzuLIST || dataType == KuzuSTRUCT || dataType == KuzuMAP {
		jsonBytes, err := json.Marshal(converted)
		if err != nil {
			return "", fmt.Errorf("failed to serialize complex property: %w", err)
		}
		return string(jsonBytes), nil
	}

	// For simple types, convert to string
	return fmt.Sprintf("%v", converted), nil
}

// DeserializeComplexProperty deserializes JSON properties back to Go types
func (pc *PropertyConverter) DeserializeComplexProperty(jsonStr string) (interface{}, error) {
	var result interface{}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return nil, fmt.Errorf("failed to deserialize complex property: %w", err)
	}
	return result, nil
}
