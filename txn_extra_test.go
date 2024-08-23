package memdb

import (
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestTxn_Count(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true) // Readonly
	total := 100
	for i := 0; i < total; i++ {
		obj := testObj()
		obj.ID = strconv.Itoa(i)
		obj.Foo = strconv.Itoa(i % 7)
		if i >= 50 {
			obj.Int = 8
		}

		txn.Insert("main", obj)
	}
	txn.Commit()

	reader := db.Txn(false)
	cnt, err := reader.Count("main")
	require.NoError(t, err)
	require.Equal(t, cnt, total)

	cnt, err = reader.CountBy("main", "foo", &CountByOpt{})
	require.NoError(t, err)
	require.Equal(t, cnt, 100)

	countOpt := &CountByOpt{IndexValue: []interface{}{"5"}}
	cnt, err = reader.CountBy("main", "foo", countOpt)
	require.NoError(t, err)
	require.Equal(t, cnt, 14)

	countOpt.Filter = func(val interface{}) bool {
		item := val.(*TestObject)
		return item.Int != 8
	}

	cnt, err = reader.CountBy("main", "foo", countOpt)
	require.NoError(t, err)
	require.Equal(t, cnt, 7)

}

func TestTxn_GetOption(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true) // Readonly
	total := 100
	for i := 0; i < total; i++ {
		obj := testObj()
		obj.ID = strconv.Itoa(i)
		obj.Foo = strconv.Itoa(i % 7)
		if i >= total/2 {
			obj.Int = 8
		}

		txn.Insert("main", obj)
	}
	txn.Commit()

	reader := db.Txn(false)
	cnt, err := reader.Count("main")
	require.NoError(t, err)
	require.Equal(t, cnt, 100)

	opt := &GetOpt{Limit: 100, Offset: 2}
	items, err := reader.GetOption("main", "foo", opt)
	require.NoError(t, err)
	require.Len(t, items, 98)

	opt.Offset, opt.Limit = 90, 3
	items, err = reader.GetOption("main", "foo", opt)
	require.NoError(t, err)
	require.Len(t, items, 3)

	// search by foo:5
	opt.IndexValue = []interface{}{"5"}
	items, err = reader.GetOption("main", "foo", opt)
	require.NoError(t, err)
	require.Len(t, items, 0)

	opt.Offset, opt.Limit = 10, 3
	items, err = reader.GetOption("main", "foo", opt)
	require.NoError(t, err)
	require.Len(t, items, 3)
	idStart := 7*10 + 5
	for i, item := range items {
		obj := item.(*TestObject)
		require.Equal(t, obj.ID, strconv.Itoa(idStart+i*7))
	}

	// search by foo:5, int :8
	opt.Offset, opt.Limit = 5, 10
	opt.Filter = func(val interface{}) bool {
		item := val.(*TestObject)
		return item.Int != 8
	}
	items, err = reader.GetOption("main", "foo", opt)
	require.NoError(t, err)
	require.Len(t, items, 2)

	// desc search foo:5, int :8
	opt.Offset, opt.Limit = 0, 10
	opt.Desc = true
	items, err = reader.GetOption("main", "foo", opt)
	require.NoError(t, err)
	require.Len(t, items, 7)
	ret := []string{"96", "89", "82", "75", "68", "61", "54"}
	for i, item := range items {
		node := item.(*TestObject)
		require.Equal(t, ret[i], node.ID)
	}

}

func Benchmark_GetOption(b *testing.B) {
	db, _ := NewMemDB(testValidSchema())
	txn := db.Txn(true) // Readonly
	total := 100000
	for i := 0; i < total; i++ {
		obj := testObj()
		obj.ID = strconv.Itoa(i)
		obj.Foo = strconv.Itoa(i % 7)
		txn.Insert("main", obj)
	}
	txn.Commit()

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		reader := db.Txn(false)
		opt := &GetOpt{Offset: 2, Limit: 100}
		items, err := reader.GetOption("main", "foo", opt)
		require.NoError(b, err)
		require.Len(b, items, 100)

		opt2 := &GetOpt{Offset: total - 100, Limit: total}
		items, err = reader.GetOption("main", "foo", opt2)
		require.NoError(b, err)
		require.Len(b, items, 100)
	}
	b.StopTimer()

}
