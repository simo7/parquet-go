package protomarshaller

import (
	"testing"

	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fraugster/parquet-go/parquetschema"
	pb "github.com/simo7/protoc-gen-parquet/examples"
)

func TestMarshaller_MarshalParquet(t *testing.T) {
	testData := []struct {
		EmitDefaults        bool
		UnknownEnumIDPrefix string
		Input               *pb.Person
		ExpectedOutput      map[string]interface{}
	}{
		{
			EmitDefaults: false,
			Input: &pb.Person{
				Name: "name",
				Age:  18,
			},
			ExpectedOutput: map[string]interface{}{"name": []byte("name"), "age": int32(18)},
		},
		{
			EmitDefaults: true,
			Input: &pb.Person{
				Name:        "name",
				Age:         18,
				CreatedAt:   1587479323999999999,
				UpdatedAt:   &timestamppb.Timestamp{Seconds: 1587479323, Nanos: 999999999},
				GeneratedAt: &timestamppb.Timestamp{Seconds: 1587479323, Nanos: 999999999},
			},
			ExpectedOutput: map[string]interface{}{
				"addresses": map[string]interface{}{
					"list": []map[string]interface{}{},
				},
				"age":          int32(18),
				"created_at":   int64(1587479323999999999),
				"updated_at":   int64(1587479323999),
				"generated_at": int64(1587479323999999),
				"name":         []byte("name"),
			},
		},
		{
			EmitDefaults: false,
			Input: &pb.Person{
				Addresses: []string{"address1", "address2", "a"},
			},
			ExpectedOutput: map[string]interface{}{
				"addresses": map[string]interface{}{
					"list": []map[string]interface{}{
						{
							"element": []byte("address1"),
						},
						{
							"element": []byte("address2"),
						},
						{
							"element": []byte("a"),
						},
					},
				},
			},
		},
		{
			EmitDefaults: true,
			Input: &pb.Person{
				Addresses: []string{"address1", "address2", "a"},
			},
			ExpectedOutput: map[string]interface{}{
				"addresses": map[string]interface{}{
					"list": []map[string]interface{}{
						{
							"element": []byte("address1"),
						},
						{
							"element": []byte("address2"),
						},
						{
							"element": []byte("a"),
						},
					},
				},
				"age":        int32(0),
				"created_at": int64(0),
				// "update_at":       int64(0),
				"name": []byte(""),
			},
		},
		{
			EmitDefaults: false,
			Input: &pb.Person{
				Phones: []*pb.Person_PhoneNumber{
					{
						Number:   int32(123123),
						Carriers: []string{"carrier1", "carrier2"},
						Type:     pb.Person_WORK,
					},
					{
						Number: int32(123123),
						Type:   4,
					},
				},
			},
			ExpectedOutput: map[string]interface{}{
				"phones": map[string]interface{}{
					"list": []map[string]interface{}{
						{
							"element": map[string]interface{}{
								"carriers": map[string]interface{}{
									"list": []map[string]interface{}{
										{"element": []byte("carrier1")},
										{"element": []byte("carrier2")}},
								},
								"number": int32(123123),
								"type":   []byte("WORK"),
							},
						},
						{
							"element": map[string]interface{}{
								"number": int32(123123),
								"type":   []byte("UNKNOWN"),
							},
						},
					},
				},
			},
		},
		{
			EmitDefaults:        false,
			UnknownEnumIDPrefix: "_UNKNOWN_ENUM_ID_",
			Input: &pb.Person{
				Phones: []*pb.Person_PhoneNumber{
					{
						Number:   int32(123123),
						Carriers: []string{"carrier1", "carrier2"},
						Type:     pb.Person_WORK,
					},
					{
						Number: int32(123123),
						Type:   4,
					},
				},
			},
			ExpectedOutput: map[string]interface{}{
				"phones": map[string]interface{}{
					"list": []map[string]interface{}{
						{
							"element": map[string]interface{}{
								"carriers": map[string]interface{}{
									"list": []map[string]interface{}{
										{"element": []byte("carrier1")},
										{"element": []byte("carrier2")}},
								},
								"number": int32(123123),
								"type":   []byte("WORK"),
							},
						},
						{
							"element": map[string]interface{}{
								"number": int32(123123),
								"type":   []byte("_UNKNOWN_ENUM_ID_4"),
							},
						},
					},
				},
			},
		},
		{
			EmitDefaults: true,
			Input: &pb.Person{
				Phones: []*pb.Person_PhoneNumber{
					{
						Number:   int32(123123),
						Carriers: []string{"carrier1", "carrier2"},
						Type:     pb.Person_WORK,
					},
					{
						Number: int32(123123),
						Type:   pb.Person_HOME,
					},
				},
			},
			ExpectedOutput: map[string]interface{}{
				"phones": map[string]interface{}{
					"list": []map[string]interface{}{
						{
							"element": map[string]interface{}{
								"carriers": map[string]interface{}{
									"list": []map[string]interface{}{
										{"element": []byte("carrier1")},
										{"element": []byte("carrier2")}},
								},
								"number": int32(123123),
								"type":   []byte("WORK"),
							},
						},
						{
							"element": map[string]interface{}{
								"carriers": map[string]interface{}{
									"list": []map[string]interface{}{},
								},
								"number": int32(123123),
								"type":   []byte("HOME"),
							},
						},
					},
				},
				"addresses": map[string]interface{}{
					"list": []map[string]interface{}{},
				},
				"age":        int32(0),
				"created_at": int64(0),
				// "update_at":       int64(0),
				"name": []byte(""),
			},
		},
	}

	sd, err := parquetschema.ParseSchemaDefinition(pb.ParquetSchema)
	require.NoError(t, err, "parsing schema failed")

	for idx, tt := range testData {
		obj := interfaces.NewMarshallObject(nil)
		m := &Marshaller{
			Obj:                 tt.Input,
			SchemaDef:           sd,
			EmitDefaults:        tt.EmitDefaults,
			UnknownEnumIDPrefix: tt.UnknownEnumIDPrefix,
		}
		err = m.MarshalParquet(obj)
		assert.NoError(t, err, "%d. could not marshal", idx)
		assert.Equal(t, tt.ExpectedOutput, obj.GetData(), "%d. output mismatch", idx)
	}
}
