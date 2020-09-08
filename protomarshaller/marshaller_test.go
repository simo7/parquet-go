package protomarshaller

import (
	"io/ioutil"
	"testing"

	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fraugster/parquet-go/parquetschema"
	pb "github.com/fraugster/parquet-go/protomarshaller/proto"
)

func TestMarshaller_MarshalParquet(t *testing.T) {
	testData := []struct {
		Input          *pb.Person
		ExpectedOutput map[string]interface{}
	}{
		{
			Input: &pb.Person{
				Name: "name",
				Age:  18,
			},
			ExpectedOutput: map[string]interface{}{"name": []byte("name"), "age": int32(18)},
		},
		{
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
								"number": int32(123123),
								"type":   []byte("HOME"),
							},
						},
					},
				},
			},
		},
	}

	b, err := ioutil.ReadFile("proto/person.schema")
	require.NoError(t, err, "the parquet schema file could not be read")
	sd, err := parquetschema.ParseSchemaDefinition(string(b))
	require.NoError(t, err, "parsing schema failed")

	for idx, tt := range testData {
		obj := interfaces.NewMarshallObject(nil)
		m := &Marshaller{Obj: tt.Input, SchemaDef: sd}
		err = m.MarshalParquet(obj)
		assert.NoError(t, err, "%d. could not marshal", idx)
		assert.Equal(t, tt.ExpectedOutput, obj.GetData(), "%d. output mismatch", idx)
	}
}
