package disktable

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"golang.org/x/exp/constraints"
)

type testLookups struct {
	Indexes [][]string `json:"indexes"`
	Results []uint64   `json:"results"`
}

func TestDiskFetchWrite(t *testing.T) {
	indexes := NewIndexes(
		&Index{Name: "First Name", AllowDuplicates: true},
		&Index{Name: "Last Name", AllowDuplicates: true},
		&Index{Name: "ID"},
	)

	tests := []struct {
		desc      string
		dataDir   string
		writeErr  bool
		searchErr bool
	}{
		{
			desc:    "Success",
			dataDir: "testing/good",
		},
	}

	for _, test := range tests {
		dir := filepath.Join(os.TempDir(), "diskdb_testing")
		os.RemoveAll(dir)

		writer, err := New(dir, WithIndexes(indexes))
		if err != nil {
			panic(err)
		}

		// Read everything in entries.csv and put them in our table.
		r, err := os.Open(filepath.Join(test.dataDir, "entries.csv"))
		if err != nil {
			panic(err)
		}
		defer r.Close()

		scanner := bufio.NewScanner(r)

		i := 0
		for scanner.Scan() {
			i++
			uintBytes := make([]byte, 8)
			sp := strings.Split(scanner.Text(), ",")
			firstLast := strings.Split(sp[0], " ")
			id, err := strconv.ParseUint(sp[1], 10, 64)
			if err != nil {
				panic(err)
			}
			endian.PutUint64(uintBytes, id)

			insert := indexes.Insert(uintBytes)
			insert = insert.AddIndexKey("First Name", UnsafeGetBytes(firstLast[0]))
			insert = insert.AddIndexKey("Last Name", UnsafeGetBytes(firstLast[1]))
			insert = insert.AddIndexKey("ID", uintBytes)
			err = writer.WriteData(insert)

			if err != nil && !test.writeErr {
				t.Fatalf("TestDiskFetchWrite(%s): got write error == %q, want nil", test.desc, err)
			}
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}

		if err := writer.Close(); err != nil {
			panic(err)
		}

		table, err := Open(dir)
		if err != nil {
			panic(err)
		}

		if table.Len() != uint64(i) {
			t.Errorf("TestDiskFetchWrite(%s): table.Len(): got %d, want %d", test.desc, table.Len(), i)
		}

		// Check queries and results for this test set.
		b, err := os.ReadFile(filepath.Join(test.dataDir, "lookups.json"))
		if err != nil {
			panic(err)
		}
		lookups := []testLookups{}
		if err := json.Unmarshal(b, &lookups); err != nil {
			panic(err)
		}

		for _, l := range lookups {
			lookupArgs := make([]Lookup, 0, len(l.Indexes))
			for _, indexLookup := range l.Indexes {
				var l Lookup
				if indexLookup[0] == "ID" {
					i, err := strconv.Atoi(indexLookup[1])
					if err != nil {
						panic(err)
					}
					b := make([]byte, 8)
					endian.PutUint64(b, uint64(i))
					l = Lookup{
						IndexName: indexLookup[0],
						Key:       b,
					}
				} else {
					l = Lookup{
						IndexName: indexLookup[0],
						Key:       []byte(indexLookup[1]),
					}
				}
				lookupArgs = append(lookupArgs, l)
			}
			ch, err := table.Fetch(context.Background(), lookupArgs[0], lookupArgs[1:]...)
			if err != nil {
				t.Fatalf("TestDiskFetchWrite(%s): unexpected .Fetch() error: %s", test.desc, err)
			}
			allResults := []uint64{}
			for result := range ch {
				allResults = append(allResults, endian.Uint64(result.Value))
			}
			sortSlice(allResults)
			if diff := pretty.Compare(l.Results, allResults); diff != "" {
				t.Fatalf("TestDiskFetchWrite(%s): -want/+got:\n%s", test.desc, diff)
			}
		}

	}
}

func sortSlice[T constraints.Ordered](s []T) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func TestFetchAll(t *testing.T) {
	count := 100000
	dir := filepath.Join(os.TempDir(), "diskdb_testing")
	log.Println("dir is: ", dir)
	os.RemoveAll(dir)

	writer, err := New(dir)
	if err != nil {
		panic(err)
	}

	expect := map[uint32]bool{}
	for i := 0; i < count; i++ {
		b := make([]byte, 8)
		endian.PutUint32(b, uint32(i))
		dataOnly := NewInsert(b)
		if err := writer.WriteData(dataOnly); err != nil {
			panic(err)
		}
		expect[uint32(i)] = false
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}

	table, err := Open(dir)
	if err != nil {
		panic(err)
	}

	results, err := table.FetchAll(context.Background())
	if err != nil {
		panic(err)
	}

	for result := range results {
		if result.Err != nil {
			panic(err)
		}

		i := endian.Uint32(result.Value)
		expect[i] = true
	}

	for i := 0; i < count; i++ {
		if !expect[uint32(i)] {
			t.Errorf("TestFetchAll: value %d was not written", i)
		}
	}
}

func TestFinalKey(t *testing.T) {
	tests := []struct {
		desc    string
		b       []byte
		counter uint64
		want    keyData
	}{
		{
			desc: "No counter",
			b:    []byte("hello world"),
			want: keyData{
				key:    []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 11},
				prefix: []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100},
			},
		},
		{
			desc:    "counter",
			b:       []byte("hello world"),
			counter: 1000000,
			want: keyData{
				key:    []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 15, 66, 64, 0, 11},
				prefix: []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100},
				suffix: []byte{0, 0, 0, 0, 0, 15, 66, 64},
			},
		},
	}

	for _, test := range tests {
		got := finalKey(test.b, test.counter)

		gotStruct, err := getPrefixSuffix(got)
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(gotStruct.key, test.want.key) {
			t.Errorf("TestFinalKey(%s): did not get expected key", test.desc)
		}
		if !bytes.Equal(gotStruct.prefix, test.want.prefix) {
			t.Errorf("TestFinalKey(%s): did not get expected prefix", test.desc)
		}
		if !bytes.Equal(gotStruct.suffix, test.want.suffix) {
			t.Errorf("TestFinalKey(%s): did not get expected suffix", test.desc)
		}
	}
}

func TestGetPrefixSuffix(t *testing.T) {
	tests := []struct {
		desc string
		key  []byte
		err  bool
	}{
		{
			desc: "error: key length is too small",
			key:  []byte{11, 0, 104, 101, 108, 108, 111, 32, 119, 111},
			err:  true,
		},
		{
			desc: "error: suffix is too small",
			key:  []byte{11, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 64, 66, 15, 0, 0, 0, 0},
			err:  true,
		},
		{
			desc: "error: suffix is too large",
			key:  []byte{11, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 64, 66, 15, 0, 0, 0, 0, 0, 0},
			err:  true,
		},
		{
			desc: "error: prefix length too small",
			key:  []byte{11, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 64, 66, 15, 0, 0, 0, 0, 0},
			err:  true,
		},
		{
			desc: "error: prefix length is too big",
			key:  []byte{11, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 111, 64, 66, 15, 0, 0, 0, 0, 0},
			err:  true,
		},
		{
			desc: "success",
			key:  []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 15, 66, 64, 0, 11},
		},
	}

	for _, test := range tests {
		_, err := getPrefixSuffix(test.key)
		switch {
		case err == nil && test.err:
			t.Errorf("TestGetPrefixSuffix(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestGetPrefixSuffix(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}
	}
}
