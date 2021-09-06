package protomarshaller

import (
	"fmt"
	"time"

	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	parquetOpts "github.com/simo7/protoc-gen-parquet/parquet_options"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Marshaller is a custom marshaller for protobuf structs
type Marshaller struct {
	Obj       proto.Message
	SchemaDef *parquetschema.SchemaDefinition
	// EmitDefaults sets default values for unpopulated fields of populated messages.
	// Default is false.
	EmitDefaults bool
	// UnknownEnumIDPrefix forms the field's value by prefixing the enum ID when
	// this is unknown. Eg. "_UNKNOWN_ENUM_ID_" + "32".
	// When not set the the value will correspond to the 0 ID.
	UnknownEnumIDPrefix string
}

// MarshalParquet hydrates a MarshalObject record from a protobuf struct
func (m *Marshaller) MarshalParquet(record interfaces.MarshalObject) error {
	return m.marshal(record, m.Obj.ProtoReflect(), m.SchemaDef)
}

func (m *Marshaller) marshal(record interfaces.MarshalObject, message protoreflect.Message, schemaDef *parquetschema.SchemaDefinition) error {
	if err := m.decodeMessage(record, message, schemaDef); err != nil {
		return err
	}

	return nil
}

func (m *Marshaller) decodeMessage(record interfaces.MarshalObject, message protoreflect.Message, schemaDef *parquetschema.SchemaDefinition) error {
	var err error

	var f = func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		fieldName := string(fd.Name())
		schemaDef := schemaDef.SubSchema(fieldName)
		field := record.AddField(fieldName)

		err = m.decodeValue(field, value, schemaDef, fd)
		if err != nil {
			return false
		}
		return true
	}

	if m.EmitDefaults {
		rangeEmitDefaults(message, f)
	} else {
		message.Range(f)
	}

	return err
}

// range over fields (populated or not) of populated messages.
func rangeEmitDefaults(m protoreflect.Message, f func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool) {
	fds := m.Descriptor().Fields()
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		if (m.Has(fd) || fd.Kind().String() != "message") && !f(fd, m.Get(fd)) {
			return
		}
	}
}

func (m *Marshaller) decodeValue(field interfaces.MarshalElement, value protoreflect.Value, schemaDef *parquetschema.SchemaDefinition, fd protoreflect.FieldDescriptor) error {
	elem := schemaDef.SchemaElement()
	if elem == nil {
		return fmt.Errorf("no schema element present on the schema definition for field: %s", fd.FullName())
	}

	if fd.IsList() && elem.GetConvertedType() == parquet.ConvertedType_LIST {
		return m.decodeRepeated(field, value, schemaDef, fd)
	}

	switch fd.Kind() {
	case protoreflect.BoolKind:
		field.SetBool(value.Bool())
		return nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		field.SetInt32(int32(value.Int()))
		return nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		field.SetInt64(value.Int())
		return nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		field.SetInt32(int32(value.Uint()))
		return nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		field.SetInt64(int64(value.Uint()))
		return nil
	case protoreflect.FloatKind:
		field.SetFloat32(float32(value.Float()))
		return nil
	case protoreflect.DoubleKind:
		field.SetFloat64(value.Float())
		return nil
	case protoreflect.EnumKind:
		enumNumber := value.Enum()
		enumValue := fd.Enum().Values().ByNumber(enumNumber)

		var enumName string
		if enumValue != nil {
			enumName = string(enumValue.Name())
			field.SetByteArray([]byte(enumName))
			return nil
		}
		if m.UnknownEnumIDPrefix == "" {
			enumValue = fd.Enum().Values().ByNumber(0)
			enumName = string(enumValue.Name())
		}
		if m.UnknownEnumIDPrefix != "" {
			enumName = fmt.Sprintf("%s%d", m.UnknownEnumIDPrefix, enumNumber)
		}

		field.SetByteArray([]byte(enumName))
		return nil
	case protoreflect.StringKind:
		field.SetByteArray([]byte(value.String()))
		return nil
	case protoreflect.BytesKind:
		return m.decodeByteSliceOrArray(field, value, schemaDef)
	case protoreflect.MessageKind, protoreflect.GroupKind:

		if value.Message().Type().Descriptor().FullName() == "google.protobuf.Timestamp" {
			unixtime, err := m.decodeTimestamp(fd, value)
			if err != nil {
				return err
			}
			field.SetInt64(unixtime)
			return nil
		}

		return m.decodeMessage(field.Group(), value.Message(), schemaDef)
	default:
		return fmt.Errorf("unsupported type %s", fd.Kind())
	}
}

func (m *Marshaller) decodeByteSliceOrArray(field interfaces.MarshalElement, value protoreflect.Value, schemaDef *parquetschema.SchemaDefinition) error {
	v := value.Bytes()

	if elem := schemaDef.SchemaElement(); elem.LogicalType != nil && elem.GetLogicalType().IsSetUUID() {
		if len(v) != 16 {
			return fmt.Errorf("field is annotated as UUID but length is %d", len(v))
		}
	}

	field.SetByteArray(v)

	return nil
}

func (m *Marshaller) decodeRepeated(field interfaces.MarshalElement, value protoreflect.Value, schemaDef *parquetschema.SchemaDefinition, fd protoreflect.FieldDescriptor) error {
	if elem := schemaDef.SchemaElement(); elem.GetConvertedType() != parquet.ConvertedType_LIST {
		return fmt.Errorf("decoding list but schema element %s is not annotated as LIST", elem.GetName())
	}

	listSchemaDef := schemaDef.SubSchema("list")
	elemSchemaDef := listSchemaDef.SubSchema("element")

	list := field.List()
	values := value.List()

	for i := 0; i < values.Len(); i++ {
		f := list.Add()
		v := values.Get(i)
		if err := m.decodeValue(f, v, elemSchemaDef, fd); err != nil {
			return err
		}
	}

	return nil
}

func (m *Marshaller) decodeTimestamp(fd protoreflect.FieldDescriptor, value protoreflect.Value) (int64, error) {
	secDesc := value.Message().Descriptor().Fields().ByName("seconds")
	secs := value.Message().Get(secDesc)
	nanoDesc := value.Message().Descriptor().Fields().ByName("nanos")
	nanos := value.Message().Get(nanoDesc)

	optVal := proto.GetExtension(fd.Options(), parquetOpts.E_FieldOpts)
	if optVal == nil {
		return time.Unix(secs.Int(), nanos.Int()).UTC().UnixNano(), nil
	}

	timestampType := optVal.(*parquetOpts.FieldOptions).GetTimestampType()

	switch timestampType {
	case parquetOpts.TimestampType_TIMESTAMP_MILLIS:
		return time.Unix(secs.Int(), nanos.Int()).UTC().UnixNano() / int64(time.Millisecond), nil
	case parquetOpts.TimestampType_TIMESTAMP_MICROS:
		return time.Unix(secs.Int(), nanos.Int()).UTC().UnixNano() / int64(time.Microsecond), nil
	case parquetOpts.TimestampType_TIMESTAMP_NANOS:
		return time.Unix(secs.Int(), nanos.Int()).UTC().UnixNano(), nil
	default:
		return 0, fmt.Errorf("unknown timestamp type: %s", timestampType.String())
	}
}
