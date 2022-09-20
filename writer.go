package disktable

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/johnsiilver/calloptions"

	/*
		badger "github.com/outcaste-io/badger/v3"
		badgerOptions "github.com/outcaste-io/badger/v3/options"
		"github.com/outcaste-io/badger/v3/pb"
		"github.com/outcaste-io/ristretto/z"
	*/

	badger "github.com/dgraph-io/badger/v3"
	badgerOptions "github.com/dgraph-io/badger/v3/options"
	"github.com/dgraph-io/ristretto/z"
)

const dbVersion = "badger/v3"

// tableDef defines what is in the table. Tables always have primary data that is indexed
// by an integer, so that is not included. This only covers the other "columns" that
// represents our indexes.
type tableDef struct {
	// DBVersion is the version of the DB in use.
	DBVersion string
	// Columns details the column information.
	Columns []columnDef
	// Length is the number of items in the table.
	Length uint64
}

func (t *tableDef) marshal(pathDir string) error {
	t.DBVersion = dbVersion
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(pathDir, "tabledef.json"), b, 0600); err != nil {
		return err
	}
	return nil
}

func (t *tableDef) unmarshal(pathDir string) error {
	r, err := os.Open(filepath.Join(pathDir, "tabledef.json"))
	if err != nil {
		return err
	}

	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	if err := dec.Decode(t); err != nil {
		unsupported := &json.UnsupportedValueError{}
		if errors.Is(err, unsupported) {
			return fmt.Errorf("current tableDef does not support a found field: %w", err)
		}
		return err
	}
	return nil
}

type columnDef struct {
	// Name is the name of index column.
	Name string
	// Path is the path to that BadgerDB.
	Path string
	// AllowDuplicates tells if the index allows duplicate keys.
	AllowDuplicates bool
}

// Writer represents our disk database.
type Writer struct {
	// dirPath is the path to the root directory for this database.
	dirPath string
	// inmemory indicates to run the DB in memory instead of persistence to disk.
	inmemory bool
	// primary is the primary data table.
	primary *Index
	// indexes are the set of indexes by name.
	indexes map[string]*Index
	// indexList is the indexes in the order they were added.
	indexList []*Index

	// wg is the number or writes that are happening.
	wg sync.WaitGroup

	// writeCh gives us access to a writer goroutine pool.
	writeCh chan func()

	logger badger.Logger
}

type Indexes struct {
	indexes []*Index
	mapping map[string]int
	reverse map[int]string
	Err     error
}

func NewIndexes(indexes ...*Index) Indexes {
	m := map[string]int{}
	r := map[int]string{}
	for i, index := range indexes {
		m[index.Name] = i
		r[i] = index.Name
	}
	return Indexes{indexes: indexes, mapping: m}
}

// Insert creates an Insert type that can be used to write data to the database.
// See Insert for more information.
func (i Indexes) Insert(value []byte) Insert {
	return newInsert(i, value)
}

// Index represents an index on our databse.
type Index struct {
	// Name of the index. This must be unique.
	Name string
	// AllowDuplicates indicates if this index allows duplicate keys for the index.
	AllowDuplicates bool

	// path is the path to the index.
	path string

	// db is access to the database for this index.
	db *badger.DB

	toWrite [][2][]byte

	// bufferPool has a set of buffers for resuse when writing to this index.
	buffPool chan *z.Buffer

	// counter is a number used in the badgerDB key when this is the primary key or
	// Name == "primary".
	counter atomic.Uint64

	// mu protects keyCount.
	mu sync.Mutex

	// keyCount keeps count of how many times a particular value has been seen
	// so that we can increment a counter when writing the key to badgerdb. Aka
	// so you can get "samekey.1", "samekey.2", ... map key is string because you
	// can't use []byte. Only used if AllowDuplicates is true.
	keyCount map[string]*atomic.Uint64
}

// type WriteOption func(db *Writer) error

// WriteOption is optional arguments for New().
type WriteOption interface {
	writeOption()
}

// Indexes provide the indexes that will be used on this database. Can be used with:
//   - New()
func WithIndexes(indexes Indexes) interface {
	WriteOption
	calloptions.CallOption
} {
	return struct {
		WriteOption
		calloptions.CallOption
	}{
		CallOption: calloptions.New(
			func(a any) error {
				db, ok := a.(*Writer)
				if !ok {
					return fmt.Errorf("WithIndexes can only be called on New(), got %T", a)
				}

				for _, index := range indexes.indexes {
					if strings.TrimSpace(index.Name) != index.Name {
						return fmt.Errorf("index name cannot have space surrounding it")
					}
					if index.Name == "" {
						return fmt.Errorf("index cannot be an empty string")
					}
					if index.Name == "primary" {
						return fmt.Errorf("index cannot be named primary(reserved")
					}
					if _, ok := db.indexes[index.Name]; ok {
						return fmt.Errorf("cannot have duplicate index name %q", index.Name)
					}
					if index.AllowDuplicates {
						index.keyCount = map[string]*atomic.Uint64{}
					}
					index.buffPool = make(chan *z.Buffer, 100)

					db.indexes[index.Name] = index
					db.indexList = append(db.indexList, index)
				}
				return nil
			},
		),
	}
}

// WithInMemory causes the DB to run from memory with no disk persistence. Great for
// tests. Can be used with:
//   - New()
func WithInMemory() interface {
	WriteOption
	calloptions.CallOption
} {
	return struct {
		WriteOption
		calloptions.CallOption
	}{
		CallOption: calloptions.New(
			func(a any) error {
				x, ok := a.(*Writer)
				if !ok {
					return fmt.Errorf("WithInMemory() can only be called on a *Writer")
				}
				x.inmemory = true
				return nil
			},
		),
	}
}

// WithLogger sets the logger for badger. By default this is goes to null. Can be used in:
//   - New()
//   - Open()
func WithLogger(l badger.Logger) interface {
	WriteOption
	OpenOption
	calloptions.CallOption
} {
	return struct {
		WriteOption
		OpenOption
		calloptions.CallOption
	}{
		CallOption: calloptions.New(
			func(a any) error {
				switch v := a.(type) {
				case *Writer:
					v.logger = l
				case *Table:
					v.logger = l
				default:
					return fmt.Errorf("WithLogger can only be used with New() and Open(), had type %T", a)
				}
				return nil
			},
		),
	}
}

var writeOpts = badger.DefaultOptions(
	"",
).WithBlockCacheSize(
	2147483648,
).WithBypassLockGuard(
	true,
).WithCompactL0OnClose(
	true,
).WithCompression(
	badgerOptions.Snappy,
).WithNumCompactors(
	runtime.NumCPU(),
).WithNumGoroutines(
	runtime.NumCPU(),
).WithNumVersionsToKeep(
	0,
)

// New creates a new instance of our table store. "dirPath" is the path to a directory
// that will be created. This must not already exist.
func New(dirPath string, options ...WriteOption) (*Writer, error) {
	if _, err := os.Stat(dirPath); err == nil {
		return nil, fmt.Errorf("path %q already exists", dirPath)
	}

	db := &Writer{dirPath: dirPath, indexes: map[string]*Index{}, logger: nullLogger{}}

	if err := calloptions.ApplyOptions(db, options); err != nil {
		return nil, err
	}

	for _, index := range db.indexes {
		index.path = filepath.Join(dirPath, "indexes", index.Name)
	}

	// Add our primary data table.
	db.primary = &Index{Name: "primary", path: filepath.Join(dirPath, "primary")}

	for _, index := range db.indexes {
		if err := os.MkdirAll(index.path, 0700); err != nil {
			return nil, fmt.Errorf("problem creating file path %q: %w", index.path, err)
		}
	}

	if err := db.createTables(); err != nil {
		os.RemoveAll(dirPath)
		return nil, err
	}

	db.writeCh = make(chan func(), len(db.indexList)+1)

	for i := 0; i < len(db.indexList)+1; i++ {
		go db.poolMember(db.writeCh)
	}
	return db, nil
}

func (d *Writer) createTables() (err error) {
	defer func() {
		if err != nil {
			for _, index := range d.indexes {
				if index.db != nil {
					index.db.Close()
				}
			}
		}
	}()

	// Setup primary table.
	if err := d.setupIndex(d.primary); err != nil {
		return err
	}

	// Setup index tables.
	for _, index := range d.indexes {
		if err := d.setupIndex(index); err != nil {
			return err
		}
	}
	return nil
}

func (d *Writer) setupIndex(index *Index) error {
	badgeOpts := writeOpts.WithDir(index.path).WithValueDir(index.path).WithLogger(d.logger)
	if d.inmemory {
		badgeOpts = badgeOpts.WithInMemory(true)
	}

	table, err := badger.Open(badgeOpts)
	if err != nil {
		return err
	}

	index.db = table
	return nil
}

// poolMember is a goroutine that takes functions off the channel and executes them.
func (d *Writer) poolMember(ch chan func()) {
	for f := range ch {
		f()
	}
}

// Close closes out the Writer.
func (d *Writer) Close() error {
	d.wg.Wait()      // Wait for all writes to stop.
	close(d.writeCh) // Kill all goroutines.

	if len(d.primary.toWrite) != 0 {
		err := d.primary.db.Update(
			func(txn *badger.Txn) error {
				for _, kv := range d.primary.toWrite {
					if err := txn.Set(kv[0], kv[1]); err != nil {
						return err
					}
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
	}

	for { // This logic, which looks weird, comes from the Badger documentation.
		err := d.primary.db.RunValueLogGC(0.7)
		if err == nil {
			continue
		}
		break
	}

	if err := d.primary.db.Flatten(runtime.NumCPU()); err != nil {
		return err
	}

	if err := d.primary.db.Close(); err != nil {
		return err
	}

	def := tableDef{Columns: make([]columnDef, 0, len(d.indexes)), Length: d.primary.counter.Load()}

	for _, index := range d.indexes {
		// If we still have values to write to our indexes, do that.
		if len(index.toWrite) != 0 {
			err := index.db.Update(
				func(txn *badger.Txn) error {
					for _, kv := range index.toWrite {
						if err := txn.Set(kv[0], kv[1]); err != nil {
							return err
						}
					}
					return nil
				},
			)
			if err != nil {
				return err
			}
		}

		def.Columns = append(def.Columns, columnDef{Name: index.Name, Path: index.path, AllowDuplicates: index.AllowDuplicates})

		for { // This logic, which looks weird, comes from the Badger documentation.
			err := index.db.RunValueLogGC(0.7)
			if err == nil {
				continue
			}
			break
		}

		if err := index.db.Flatten(runtime.NumCPU()); err != nil {
			return err
		}

		if err := index.db.Close(); err != nil {
			return err
		}
	}

	return def.marshal(d.dirPath)
}

// Insert represents a data insert into the table and is created from Indexes.
// You must use Insert.AddIndexKey() to all all index keys defined in Indexes.
type Insert struct {
	value   []byte
	indexes Indexes
	inserts [][]byte
	Err     error
}

// NewInsert creates a new Insert for writing into the table. This is only used when
// there are no indexes defined on the table. Otherwise you must uses Indexes.Insert().
func NewInsert(value []byte) Insert {
	return Insert{
		value: value,
	}
}

// newInsert creates a new Insert for writing into the table.
func newInsert(indexes Indexes, value []byte) Insert {
	return Insert{
		value:   value,
		indexes: indexes,
		inserts: make([][]byte, len(indexes.indexes)),
	}
}

// AddIndexKey adds a key for a given index. You must capture the returned
// Insert as AddIndexKey() does not have a pointer receiver.
func (i Insert) AddIndexKey(indexName string, key []byte) Insert {
	k, ok := i.indexes.mapping[indexName]
	if !ok {
		i.Err = fmt.Errorf("indexName %q could not be found", indexName)
		return i
	}
	i.inserts[k] = key
	return i
}

func (i Insert) validate() error {
	for k, insert := range i.inserts {
		if len(insert) == 0 {
			return fmt.Errorf("index(%s) was not set", i.indexes.reverse[k])
		}
	}
	return nil
}

// Write data writes data to our database. indexes must be in the same order when you created
// this DB and have the same number of indexes. You cannot reuse any "value" or "indexValues" passed until
// all data has been written. This is because a single WriteData() does not cause data to be written.
func (d *Writer) WriteData(insert Insert) error {
	if err := insert.validate(); err != nil {
		return err
	}
	if insert.indexes.indexes == nil && len(d.indexList) != 0 {
		return fmt.Errorf("cannot pass an insert that has no Indexes defined when table has indexes")
	}

	wg := sync.WaitGroup{}

	errCh := make(chan error, 1)

	// Write our data to the primary table.
	wg.Add(1)
	d.wg.Add(1)
	d.writeCh <- func() {
		defer d.wg.Done()
		defer wg.Done()
		if err := d.writeData(nil, insert.value, d.primary); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}
	// Write all of our index table entries.
	for i := 0; i < len(insert.indexes.indexes); i++ {
		index := insert.indexes.indexes[i]
		key := insert.inserts[i]

		wg.Add(1)
		d.wg.Add(1)
		d.writeCh <- func() {
			defer d.wg.Done()
			defer wg.Done()
			if err := d.writeData(key, nil, index); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	return nil
}

// write writes a key/value to an index. If the index is "primary", the key must be nil.
// If not primary, the value must be empty.
func (d *Writer) writeData(key, value []byte, index *Index) error {
	var (
		// indexCounter is the current number of entries in an index, including this one.
		indexCounter uint64
		// kcCounter is the number of times a duplicate key has been added to the index.
		kcCounter uint64
		// finalKeyName is the actual key we store our entry in the index under. This
		// is slightly different than the key we receive.
		finalKeyName []byte
		// primary indicates if this is the primary index we are editing. The primary
		// index contain our actual values where all others are reference tables to the
		// primary index.
		primary = false
	)

	if index.Name == "primary" {
		primary = true
		if key != nil {
			panic("writeData() to primary index must have key that is nil")
		}
	} else {
		if len(value) != 0 {
			panic("writeData() to non-primary index must have value that is empty")
		}
	}

	indexCounter = index.counter.Add(1)

	if index.AllowDuplicates {
		k := ByteSlice2String(key)

		index.mu.Lock()
		kc := index.keyCount[k]
		if kc == nil {
			kc = &atomic.Uint64{}
			index.keyCount[k] = kc
		}
		index.mu.Unlock()

		kcCounter = kc.Add(1)
	}

	switch true {
	case primary:
		finalKeyName = finalKey(nil, indexCounter)
	case index.AllowDuplicates:
		finalKeyName = finalKey(key, kcCounter)
	default:
		finalKeyName = finalKey(key, 0)
	}

	// Set our KV.
	if !primary {
		value = make([]byte, 8)
		endian.PutUint64(value, indexCounter)
	}

	index.toWrite = append(index.toWrite, [2][]byte{finalKeyName, value})
	if len(index.toWrite) == 10000 {
		txn := index.db.NewTransaction(true)

		for _, kv := range index.toWrite {
			if !index.AllowDuplicates {
				_, err := txn.Get(kv[0])
				if err == nil {
					txn.Discard()
					return fmt.Errorf("index(%s) key(%s) is a duplicate, which is not allowed in that index", index.Name, string(kv[0]))
				}
			}
			e := badger.NewEntry(kv[0], kv[1])
			if err := txn.SetEntry(e); err != nil {
				if err := txn.Commit(); err != nil {
					txn.Discard()
					return err
				}
				txn = index.db.NewTransaction(true)
				txn.SetEntry(e)
			}
		}
		if err := txn.Commit(); err != nil {
			txn.Discard()
			return err
		}
		index.toWrite = index.toWrite[0:0]
	}
	return nil
}
