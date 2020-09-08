package protomarshaller

import (
	"fmt"

	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Marshaller is a custom marshaller for protobuf structs
type Marshaller struct {
	Obj       proto.Message
	SchemaDef *parquetschema.SchemaDefinition
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

	message.Range(f)

	return err
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
		enumName := fd.Enum().Values().ByNumber(enumNumber).Name()
		field.SetByteArray([]byte(string(enumName)))
		return nil
	case protoreflect.StringKind:
		field.SetByteArray([]byte(value.String()))
		return nil
	case protoreflect.BytesKind:
		return m.decodeByteSliceOrArray(field, value, schemaDef)
	case protoreflect.MessageKind, protoreflect.GroupKind:
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
