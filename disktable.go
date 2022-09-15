/*
Package disktable provides a write-once, read-many table with index supoprt.
This is build on top of badgerDB, which is basically a key/value SSTable storage mechanism.

Let's create a table with some data:

	dir := filepath.Join(os.TempDir(), "your_table"")
	// Remove it if exists, may or may not want to do this. However you cannot
	// create a table on a directory that exists.
	os.RemoveAll(dir)

	// These are our indexes on the data. AllowDuplicates allows duplicate entries
	// in the index.
	indexes := []*Index{
		{Name: "First Name", AllowDuplicates: true},
		{Name: "Last Name", AllowDuplicates: true},
		{Name: "ID"},
	}

	w, err := New(dir, WithIndexes(indexes...))
	if err != nil {
		panic(err)
	}

	for _, data := range someData {
		b, err := proto.Marshal(data)
		if err != nil {
			panic(err)
		}
		err = w.WriteData(
			b,
			UnsafeGetBytes(data.First),
			UnsafeGetBytes(data.Last),
			NumToByte(data.ID),
		)
		if err != nil {
			panic(err)
		}
	}

	if err := w.Close(); err != nil {
		panic(err)
	}

Now let's open it and stream all records:

	table, err := Open(dir)
	if err != nil {
		panic(err)
	}

	results, err := table.FetchAll(ctx)
	if err != nil {
		panic(err)
	}

	for result := range results {
		if result.Err != nil {
			panic(err)
		}

		entry := &pb.MyData{}
		if err := proto.Unmarshal(entry, result.Value); err != nil {
			panic(err)
		}

		fmt.Println("found: ", pretty.Sprint(entry))
	}

Let's look for all entries that have the first name John:

	results, err := table.Fetch(
		ctx,
		Lookup{IndexName: "First Name", Key: UnsafeGetBytes("John")},
	)

	if err != nil {
		panic(err)
	}

	for result := range results {
		if result.Err != nil {
			panic(err)
		}

		entry := &pb.MyData{}
		if err := proto.Unmarshal(entry, result.Value); err != nil {
			panic(err)
		}

		fmt.Println("found: ", pretty.Sprint(entry))
	}
*/
package disktable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	// This marks the outcaste-io version, which is newer but gets rid of the values file.
	// badger is stable, so at this point I see no reason to move.
	/*
		badger "github.com/outcaste-io/badger/v3"
		"github.com/outcaste-io/ristretto/z"
	*/

	badger "github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
	"github.com/google/btree"
	"github.com/johnsiilver/calloptions"
)

var endian = binary.BigEndian

// Table represents our read-only table.
type Table struct {
	// primary is the primary data table.
	primary     *Index
	indexByName map[string]*Index
	// indexes is the indexes in the order they were added.
	indexes []*Index

	logger badger.Logger
}

// OpenOption is optional arguments for Open().
type OpenOption interface {
	openOption()
}

var readOpts = badger.DefaultOptions(
	"",
).WithBlockCacheSize(
	2147483648,
).WithNumGoroutines(
	runtime.NumCPU(),
).WithReadOnly(
	true,
).WithBloomFalsePositive(
	.01,
)

// Open opens an existing disktable for reading.
func Open(pathDir string, options ...OpenOption) (*Table, error) {
	if _, err := os.Stat(pathDir); err != nil {
		return nil, fmt.Errorf("pathDir %q does not exist", pathDir)
	}

	table := &Table{indexByName: map[string]*Index{}, logger: nullLogger{}}

	if err := calloptions.ApplyOptions(table, options); err != nil {
		return nil, err
	}

	td := tableDef{}
	if err := td.unmarshal(pathDir); err != nil {
		return nil, err
	}

	// Open all our index tables.
	for _, col := range td.Columns {
		badgeOpts := readOpts.WithDir(col.Path).WithValueDir(col.Path).WithLogger(table.logger)
		db, err := badger.Open(badgeOpts)
		if err != nil {
			return nil, err
		}
		index := &Index{Name: col.Name, AllowDuplicates: col.AllowDuplicates, db: db, buffPool: make(chan *z.Buffer, 100)}
		table.indexes = append(table.indexes, index)
		table.indexByName[col.Name] = index
	}

	// Open our primary data table.
	primaryPath := filepath.Join(pathDir, "primary")
	badgeOpts := readOpts.WithDir(primaryPath).WithValueDir(primaryPath).WithLogger(table.logger)
	db, err := badger.Open(badgeOpts)
	if err != nil {
		return nil, err
	}
	table.primary = &Index{Name: "primary", db: db, buffPool: make(chan *z.Buffer, 100)}

	return table, nil
}

// Get gets the i'th entry stored in the table.
func (t *Table) Get(ctx context.Context, i int64) ([]byte, error) {
	panic("not implemented")
}

// Lookup provides the Index name and the Value that needs to match for the entry
// to be returned.
type Lookup struct {
	IndexName string
	Key       []byte
}

// Result is the result of a table lookup.
type Result struct {
	Value []byte
	Err   error
}

type btreeItem struct {
	primaryKey uint64
	count      uint16
}

func (a btreeItem) Less(than btree.Item) bool {
	b := than.(btreeItem)
	return a.primaryKey < b.primaryKey
}

// Fetch retrieves specifc rows that match all index lookups. You must supply at least
// 1 lookup. You cannot currently specify multiple searches in the same index.
// If you wish to fetch all rows, use FetchAll().
func (t *Table) Fetch(ctx context.Context, lookup ...Lookup) (chan Result, error) {
	indexesSearched := map[string]bool{}

	for _, l := range lookup {
		if t.indexByName[l.IndexName] == nil {
			return nil, fmt.Errorf("index %q is not found", l.IndexName)
		}
		if indexesSearched[l.IndexName] {
			return nil, fmt.Errorf("index %q cannot be used in a search more than once", l.IndexName)
		}
		indexesSearched[l.IndexName] = true
	}

	if len(lookup) == 0 {
		return nil, fmt.Errorf("Fetch() requires lookups, otherwise use FetchAll()")
	}

	ch := make(chan Result, 1)

	go func() {
		defer close(ch)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		matches := btree.New(2)

		// fetch all rows for each index that match our lookup constraint.
		// Record those rows into a 2-3-4 tree. Any entry in the tree that has
		// len(lookup) as its value is a line we want to return.
		errCh := make(chan error, 1)
		for _, l := range lookup {
			l := l

			wg.Add(1)
			go func() {
				defer wg.Done()
				for result := range t.indexLookup(l.IndexName, l.Key) {
					if ctx.Err() != nil {
						return
					}
					if result.err != nil {
						if result.err == badger.ErrKeyNotFound {
							continue
						}
						cancel()
						select {
						case errCh <- result.err:
						default:
						}
					}
					// Add entry to btree or increase existing entry counter.
					mu.Lock()

					var counter btreeItem
					a := matches.Get(btreeItem{primaryKey: result.i})
					if a == nil {
						counter = btreeItem{primaryKey: result.i}
					} else {
						counter = a.(btreeItem)
					}
					counter.count++
					matches.ReplaceOrInsert(counter)

					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		// Did we have any errors, if so return them.
		select {
		case err := <-errCh:
			ch <- Result{Err: err}
			return
		default:
		}

		keysLookup := make([][]byte, 0, 100)

		// Pull all the rows in the index that matched and send them back.
		matches.Ascend(
			func(item btree.Item) bool {
				i := item.(btreeItem)
				if int(i.count) == len(lookup) {
					primaryKey := make([]byte, 8)
					endian.PutUint64(primaryKey, i.primaryKey)
					keysLookup = append(keysLookup, primaryKey)
					// Ok, we have enough for a bulk lookup.
					if len(keysLookup) == 1000 {
						t.primary.db.View(t.retrievePrimaryRows(keysLookup, ch))
						keysLookup = keysLookup[0:0]
					}
				}
				return true
			},
		)

		// Get any outstanding lookups that didn't have enough to do a bulk.
		if len(keysLookup) > 0 {
			t.primary.db.View(t.retrievePrimaryRows(keysLookup, ch))
		}
	}()

	return ch, nil
}

func (t *Table) retrievePrimaryRows(keys [][]byte, ch chan Result) func(txn *badger.Txn) error {
	return func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			if !bytes.Equal(item.Key(), key) {
				panic("known key search came up with different key: HUGE PROBLEM, HUGE!!!!!!")
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("problem getting value of key(%s): %w", string(key), err)
			}
			ch <- Result{Value: value}
		}
		return nil
	}
}

type lookupResult struct {
	i   uint64
	err error
}

// indexLookup returns all matching keys in an index.
func (t *Table) indexLookup(indexName string, key []byte) chan lookupResult {
	index := t.indexByName[indexName]

	realKey := finalKey(key, 0)

	ch := make(chan lookupResult, 1)
	if index.AllowDuplicates {
		// multiMatch will close the channel.
		go t.multiMatch(realKey, index, ch)
	} else {
		ch <- t.exactMatch(realKey, index)
		close(ch)
	}

	return ch
}

// exactMatch does an exact match for a key in the index. key must have been finalKey()'d.
func (t *Table) exactMatch(key []byte, index *Index) lookupResult {
	var result lookupResult

	f := func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		if !bytes.Equal(item.Key(), key) {
			panic("known key search came up with different key: HUGE PROBLEM, HUGE!!!!!!")
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("problem getting value of key(%s): %w", string(key), err)
		}
		result = lookupResult{i: endian.Uint64(value)}
		return nil
	}

	if err := index.db.View(f); err != nil {
		result.err = err
	}
	return result

}

// multiMatch matches all keys in an index that allows duplicates. key must have been finalKey()'d.
func (t *Table) multiMatch(key []byte, index *Index, ch chan lookupResult) {
	defer close(ch)

	keyData, err := getPrefixSuffix(key)
	if err != nil {
		ch <- lookupResult{err: err}
		return
	}

	tempBuff := make([]byte, 8)
	err = index.db.View(
		func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Seek(keyData.prefix); it.ValidForPrefix(keyData.prefix); it.Next() {
				item := it.Item()

				foundKeyData, err := getPrefixSuffix(item.Key())
				if err != nil {
					return fmt.Errorf("multiMatch of index(%s) had bad key(%v): %w", index.Name, item.Key(), err)
				}
				// It is possible to have a prefix match for something not exactly the same.
				// But we are looking for exact prefix matches followed by a number followed by
				// our key size. By matching prefix and key size, we can be sure of an exact match.
				// If not, throw out the data.
				if len(foundKeyData.prefix) != len(keyData.prefix) {
					continue
				}
				b, err := item.ValueCopy(tempBuff)
				if err != nil {
					return err
				}
				ch <- lookupResult{i: endian.Uint64(b)}
			}
			return nil
		},
	)
	if err != nil {
		ch <- lookupResult{err: err}
	}
}

func (t *Table) FetchAll(ctx context.Context) (chan Result, error) {
	stream := t.primary.db.NewStream()
	stream.NumGo = 16
	stream.LogPrefix = "FetchAll.Streaming"

	ch := make(chan Result, 1)
	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		for _, kv := range list.Kv {
			// TODO(jdoak): This doesn't seem to matter.
			v := make([]byte, len(kv.Value))
			copy(v, kv.Value)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- Result{Value: v}:
			}
		}
		return nil
	}

	go func() {
		defer close(ch)
		if err := stream.Orchestrate(context.Background()); err != nil {
			ch <- Result{Err: err}
			return
		}
	}()

	return ch, nil
}

// ByteSlice2String coverts bs to a string. It is no longer safe to use bs after this.
// This prevents having to make a copy of bs.
func ByteSlice2String(bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&bs))
}

// UnsafeGetBytes retrieves the underlying []byte held in string "s" without doing
// a copy. Do not modify the []byte or suffer the consequences.
func UnsafeGetBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}

// Number represents any uint*, int* or float* type.
type Number interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~float32 | ~float64
}

// NumToByte converts a number into a BigEndian []byte sequence.
func NumToByte[N Number](n N) []byte {
	buff := bytes.Buffer{}

	binary.Write(&buff, binary.BigEndian, n)
	return buff.Bytes()
}

// ByteToNum returns a number stored in b that represents N. That number should
// be encoded in BigEndian, usually by NumToByte().
func ByteToNum[N Number](b []byte) (N, error) {
	var v N
	if err := binary.Read(bytes.NewReader(b), binary.BigEndian, v); err != nil {
		return 0, err
	}
	return v, nil
}

// finalKey creates a final key out of a key and a counter. If len(key) == 0 and counter > 0,
// it returns counter as a [8]byte.
func finalKey(key []byte, counter uint64) []byte {
	var x []byte
	switch {
	case counter == 0 && len(key) == 0:
		panic("bug: can't have a finalKey() call with key == nil  and counter == 0")
	case counter == 0:
		// [key][key size(uint16)]
		x = make([]byte, 2+len(key))
		copy(x, key)
		endian.PutUint16(x[len(key):], uint16(len(key)))
		return x
	case len(key) == 0:
		// [counter(uint64)]
		x = make([]byte, 8)
		endian.PutUint64(x, counter)
		return x
	}
	// [key][counter(uint64)][key size(uint16)]
	x = make([]byte, 2+8+len(key))
	copy(x, key)
	endian.PutUint64(x[len(key):], counter)
	endian.PutUint16(x[len(key)+8:], uint16(len(key)))

	return x
}

type keyData struct {
	// key is the key unmodified.
	key []byte
	// prefix is the key prefix.
	prefix []byte
	// suffix is the suffix, which is a uint64.
	suffix []byte
}

// getPrefixSuffix assumes that you are pulling a key that was encoded with a suffix, aka
// a key in an allow duplicates index. If not, the key data is going to error or just be bad.
func getPrefixSuffix(key []byte) (keyData, error) {
	if len(key) < 3 { // 1 byte + uint16
		return keyData{}, fmt.Errorf("getPrefixSuffix encountered key of size: %d", len(key))
	}
	if len(key) >= math.MaxUint16 {
		return keyData{}, fmt.Errorf("cannot have a key > %d bytes", math.MaxUint16)
	}

	prefixSize := endian.Uint16(key[len(key)-2:])
	if int(prefixSize) > len(key)-2 {
		return keyData{}, fmt.Errorf("getPrefixSuffix encountered key with prefix size greater than the key length")
	}

	prefix := key[:prefixSize]
	var suffix []byte
	if len(key)-2 > int(prefixSize) {
		suffix = key[prefixSize : len(key)-2]
	}

	if suffix == nil {
		if len(key) != 2+int(prefixSize) {
			return keyData{}, fmt.Errorf("key is corrupt, we have a uint16 footer that lists the prefix of size %d and a suffix of uint64, but have %d bytes", prefixSize, len(key))
		}
	} else {
		if len(key) != 2+int(prefixSize)+8 {
			return keyData{}, fmt.Errorf("key is corrupt, we have a uint16 footer that lists the prefix of size %d and a suffix of uint64, but have %d bytes", prefixSize, len(key))
		}
	}

	return keyData{key: key, prefix: prefix, suffix: suffix}, nil
}

// nullLogger is used to log all the badger output to null.
type nullLogger struct{}

func (n nullLogger) Errorf(string, ...interface{})   {}
func (n nullLogger) Warningf(string, ...interface{}) {}
func (n nullLogger) Infof(string, ...interface{})    {}
func (n nullLogger) Debugf(string, ...interface{})   {}
