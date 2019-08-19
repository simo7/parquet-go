package floor

import (
	"os"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

func TestDecodeStruct(t *testing.T) {
	testData := []struct {
		Input          interface{}
		ExpectedOutput map[string]interface{}
		ExpectErr      bool
	}{
		{
			Input:          struct{ Foo int16 }{Foo: 42},
			ExpectedOutput: map[string]interface{}{"foo": int32(42)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo int }{Foo: 43},
			ExpectedOutput: map[string]interface{}{"foo": int32(43)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo int8 }{Foo: 44},
			ExpectedOutput: map[string]interface{}{"foo": int32(44)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo int32 }{Foo: 100000},
			ExpectedOutput: map[string]interface{}{"foo": int32(100000)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo uint64 }{Foo: 1125899906842624},
			ExpectedOutput: map[string]interface{}{"foo": int64(1125899906842624)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo uint }{Foo: 200000},
			ExpectedOutput: map[string]interface{}{"foo": int32(200000)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo float32 }{Foo: 42.5},
			ExpectedOutput: map[string]interface{}{"foo": float32(42.5)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo float64 }{Foo: 23.5},
			ExpectedOutput: map[string]interface{}{"foo": float64(23.5)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo byte }{Foo: 1},
			ExpectedOutput: map[string]interface{}{"foo": int32(1)},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo string }{Foo: "bar"},
			ExpectedOutput: map[string]interface{}{"foo": "bar"},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo *string }{Foo: new(string)},
			ExpectedOutput: map[string]interface{}{"foo": ""},
			ExpectErr:      false,
		},
		{
			Input:          struct{ Foo *string }{},
			ExpectedOutput: map[string]interface{}{},
			ExpectErr:      false,
		},
		{
			Input:          int(23),
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: struct {
				Foo struct {
					Bar int64
				}
				Baz  uint32
				Quux *bool
				Blub bool
			}{},
			ExpectedOutput: map[string]interface{}{"foo": map[string]interface{}{"bar": int64(0)}, "baz": int64(0), "blub": false},
			ExpectErr:      false,
		},
		{
			Input: struct {
				Foo []bool
			}{
				Foo: []bool{false, true, false},
			},
			ExpectedOutput: map[string]interface{}{"foo": []bool{false, true, false}},
			ExpectErr:      false,
		},
		{
			Input: struct {
				Foo [5]uint16
			}{
				Foo: [5]uint16{1, 1, 2, 3, 5},
			},
			ExpectedOutput: map[string]interface{}{"foo": []int32{int32(1), int32(1), int32(2), int32(3), int32(5)}},
			ExpectErr:      false,
		},
		{
			Input: struct {
				C chan int
			}{},
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: struct {
				Foo struct {
					C chan int
				}
			}{},
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: struct {
				Foo []chan int
			}{Foo: []chan int{make(chan int)}},
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
		{
			Input: &struct {
				Bla int
			}{Bla: 616},
			ExpectedOutput: map[string]interface{}{"bla": int32(616)},
			ExpectErr:      false,
		},
		{
			Input: (*struct {
				Bla int
			})(nil),
			ExpectedOutput: nil,
			ExpectErr:      true,
		},
	}

	for idx, tt := range testData {
		output, err := decodeStruct(reflect.ValueOf(tt.Input))
		if tt.ExpectErr {
			assert.Error(t, err, "%d. expected error, but found none", idx)
		} else {
			assert.NoError(t, err, "%d. expected no error, but found one", idx)
			assert.Equal(t, tt.ExpectedOutput, output, "%d. output mismatch", idx)
		}
	}
}

func TestWriteFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := goparquet.NewFileWriter(wf, goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.CreatedBy("floor-unittest"))

	sd, err := goparquet.ParseSchemaDefinition(
		`message test_msg {
			required int64 foo;
			optional binary bar (STRING);
			repeated int32 baz;
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	w.SetSchemaDefinition(sd)

	hlWriter := NewWriter(w)

	data := []struct {
		Foo int64
		Bar *string
		Baz []int32
	}{
		{23, strPtr("hello!"), []int32{23}},
		{42, strPtr("world!"), []int32{1, 1, 2, 3, 5}},
		{500, nil, nil},
		{1000, strPtr("bye!"), []int32{2, 3, 5, 7, 11}},
	}

	for idx, d := range data {
		require.NoError(t, hlWriter.Write(d), "%d. Write failed", idx)
	}

	require.NoError(t, hlWriter.Close())

	rf, err := os.Open("files/test.parquet")
	require.NoError(t, err)

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	require.NoError(t, reader.ReadRowGroup())

	require.Equal(t, int64(len(data)), reader.NumRecords())

	expectedData := []map[string]interface{}{
		{
			"foo": int64(23),
			"bar": "hello!",
			"baz": []int32{23},
		},
		{
			"foo": int64(42),
			"bar": "world!",
			"baz": []int32{1, 1, 2, 3, 5},
		},
		{
			"foo": int64(500),
		},
		{
			"foo": int64(1000),
			"bar": "bye!",
			"baz": []int32{2, 3, 5, 7, 11},
		},
	}

	for i := int64(0); i < reader.NumRecords(); i++ {
		data, err := reader.GetData()
		require.NoError(t, err, "%d. reading record failed")
		require.Equal(t, expectedData[i], data, "%d. data in parquet file differs from what's expected", i)
	}
}

func TestRepeatedGroupFile(t *testing.T) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile("files/test2.parquet", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := goparquet.NewFileWriter(wf, goparquet.CompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.CreatedBy("floor-unittest"))

	sd, err := goparquet.ParseSchemaDefinition(
		`message test_msg {
			repeated group foo {
				required int64 bla;
				optional binary bar (STRING);
			}
		}`)
	require.NoError(t, err, "parsing schema definition failed")

	t.Logf("schema definition: %s", spew.Sdump(sd))

	w.SetSchemaDefinition(sd)

	hlWriter := NewWriter(w)

	type fooType struct {
		Bla uint64
		Bar *string
	}

	data := []struct {
		Foo []fooType
	}{
		{Foo: []fooType{fooType{Bla: 23, Bar: strPtr("foobar")}}},
		{Foo: []fooType{fooType{Bla: 24, Bar: strPtr("hello")}}},
		{Foo: []fooType{fooType{Bla: 25}, fooType{Bla: 26, Bar: strPtr("bye!")}, fooType{Bla: 27}}},
	}

	for idx, d := range data {
		require.NoError(t, hlWriter.Write(d), "%d. Write failed", idx)
		decodedData, err := decodeStruct(reflect.ValueOf(d))
		require.NoError(t, err)
		t.Logf("%d. decoded struct: %s", idx, spew.Sdump(decodedData))
	}

	require.NoError(t, hlWriter.Close())

	rf, err := os.Open("files/test2.parquet")
	require.NoError(t, err)

	reader, err := goparquet.NewFileReader(rf)
	require.NoError(t, err)

	require.NoError(t, reader.ReadRowGroup())

	require.Equal(t, int64(len(data)), reader.NumRecords())

	expectedData := []map[string]interface{}{
		{
			"foo": []map[string]interface{}{
				map[string]interface{}{
					"bla": int64(23),
					"bar": "foobar",
				},
			},
		},
		{
			"foo": []map[string]interface{}{
				map[string]interface{}{
					"bla": int64(24),
					"bar": "hello",
				},
			},
		},
		{
			"foo": []map[string]interface{}{
				map[string]interface{}{
					"bla": int64(25),
				},
				map[string]interface{}{
					"bla": int64(26),
					"bar": "bye!",
				},
				map[string]interface{}{
					"bla": int64(27),
				},
			},
		},
	}

	/*
		for idx, col := range reader.Columns() {
			t.Logf("col %d: %s", idx, spew.Sdump(col))
		}
	*/

	for i := int64(0); i < reader.NumRecords(); i++ {
		data, err := reader.GetData()
		require.NoError(t, err, "%d. reading record failed")
		require.Equal(t, expectedData[i], data, "%d. data in parquet file differs from what's expected", i)
	}
}

func strPtr(s string) *string {
	return &s
}
