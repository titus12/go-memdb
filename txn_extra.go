package memdb

import iradix "github.com/titus12/go-immutable-radix"

func (txn *Txn) Count(table string) (int, error) {
	// Get the index value to scan
	indexSchema, _, err := txn.getIndexValue(table, id)
	if err != nil {
		return -1, err
	}
	// Get the indexTree itself
	indexTxnTree := txn.readableIndexTree(table, indexSchema.Name)
	return indexTxnTree.Len(), nil
}

type CountByOpt struct {
	IndexValue []interface{}
	Filter     func(val interface{}) bool
}

func (txn *Txn) CountBy(table, index string, opt *CountByOpt) (int, error) {
	// Get the index value to scan
	indexSchema, val, err := txn.getIndexValue(table, index, opt.IndexValue...)
	if err != nil {
		return -1, err
	}
	// Get the indexTree itself
	indexTxnTree := txn.readableIndexTree(table, indexSchema.Name)
	cnt := 0
	indexTxnTree.Root().WalkPrefix(val, func(k []byte, v interface{}) bool {
		if opt.Filter != nil {
			if !opt.Filter(v) {
				cnt++
			}
		} else {
			cnt++
		}

		return false
	})
	// compute count
	return cnt, nil
}

type GetOpt struct {
	IndexValue []interface{}
	Limit      int
	Offset     int
	Filter     func(val interface{}) bool
	Desc       bool
}

func (opt *GetOpt) CheckAndSetDefault() *GetOpt {
	if opt.Limit <= 0 || opt.Limit > 200 {
		opt.Limit = 200
	}
	if opt.Offset < 0 {
		opt.Offset = 0
	}
	if opt.Filter == nil {
		opt.Filter = func(val interface{}) bool {
			return false
		}
	}
	return opt
}

func (txn *Txn) GetOption(table, index string, opt *GetOpt) ([]interface{}, error) {
	// Get the index value to scan
	indexSchema, val, err := txn.getIndexValue(table, index, opt.IndexValue...)
	if err != nil {
		return nil, err
	}
	// Get the indexTree itself
	indexTxnTree := txn.readableIndexTree(table, indexSchema.Name)

	// set default param
	opt.CheckAndSetDefault()

	cnt := 0
	items := make([]interface{}, 0, opt.Limit)
	root := indexTxnTree.Root()
	walk := root.WalkPrefix
	if opt.Desc {
		walk = root.WalkBackwardsPrefix
	}
	walk(val, func(k []byte, v interface{}) bool {
		if opt.Filter(v) {
			return false
		}
		if cnt >= opt.Offset {
			items = append(items, v)
		}
		if len(items) == opt.Limit {
			return true
		}
		cnt += 1
		return false
	})

	return items, nil
}

func (txn *Txn) readableIndexTree(table, index string) *iradix.Tree {
	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)

	indexTxnTree := raw.(*iradix.Tree)
	return indexTxnTree
}
