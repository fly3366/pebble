package main

import (
	"bytes"
	"log"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cmd/pebble/gorocksdb"
)

// Adapters for Badger.
type rocksDB struct {
	db *gorocksdb.DB

	bbto  *gorocksdb.BlockBasedTableOptions
	cache *gorocksdb.Cache
	ro    *gorocksdb.ReadOptions
	wo    *gorocksdb.WriteOptions
	opts  *gorocksdb.Options
}

func newRocksDB(dir string) DB {
	opts := gorocksdb.NewDefaultOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	ro := gorocksdb.NewDefaultReadOptions()
	cache := gorocksdb.NewLRUCache(1 << 30)
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()

	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		log.Fatal(err)
	}

	return &rocksDB{
		db:    db,
		bbto:  bbto,
		ro:    ro,
		wo:    wo,
		cache: cache,
	}
}

func (b rocksDB) NewIter(opts *pebble.IterOptions) iterator {
	iter := b.db.NewIterator(b.ro)

	return &rocksdbIterator{
		iter:  iter,
		lower: opts.GetLowerBound(),
		upper: opts.GetUpperBound(),
	}
}

func (b rocksDB) NewBatch() batch {
	wb := gorocksdb.NewWriteBatch()
	return &rocksdbBatch{&b, wb}
}

func (b rocksDB) Scan(iter iterator, key []byte, count int64, reverse bool) error {
	panic("rocksDB.Scan: unimplemented")
}

func (b rocksDB) Metrics() *pebble.Metrics {
	return &pebble.Metrics{}
}

func (b rocksDB) Flush() error {
	return nil
}

type rocksdbIterator struct {
	iter *gorocksdb.Iterator

	buf   []byte
	lower []byte
	upper []byte
}

func (i *rocksdbIterator) SeekGE(key []byte) bool {
	i.iter.Seek(key)
	if !i.iter.Valid() {
		return false
	}
	if i.upper != nil && bytes.Compare(i.Key(), i.upper) >= 0 {
		return false
	}
	return true
}

func (i *rocksdbIterator) SeekLT(key []byte) bool {
	i.iter.Seek(key)
	if !i.iter.Valid() {
		return false
	}

	if i.lower != nil && bytes.Compare(i.Key(), i.lower) <= 0 {
		return false
	}

	return true
}

func (i *rocksdbIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *rocksdbIterator) Key() []byte {
	key, ok := i.iter.OKey()
	if !ok {
		log.Fatal("RDB read key err")
	}

	return key.Data()
}

func (i *rocksdbIterator) Value() []byte {
	value, ok := i.iter.OValue()
	if !ok {
		log.Fatal("RDB read value err")
	}

	copy(i.buf[:0], value.Data())
	value.Free()
	return i.buf
}

func (i *rocksdbIterator) First() bool {
	i.iter.SeekToFirst()
	return true
}

func (i *rocksdbIterator) Next() bool {
	i.iter.Next()
	if !i.iter.Valid() {
		return false
	}
	if i.upper != nil && bytes.Compare(i.Key(), i.upper) >= 0 {
		return false
	}
	return true
}

func (i *rocksdbIterator) Last() bool {
	i.iter.SeekToLast()
	return true
}

func (i *rocksdbIterator) Prev() bool {
	i.iter.Prev()
	return true
}

func (i *rocksdbIterator) Close() error {
	i.iter.Close()
	return nil
}

type rocksdbBatch struct {
	db *rocksDB

	wb *gorocksdb.WriteBatch
}

func (b rocksdbBatch) Close() error {
	return nil
}

func (b rocksdbBatch) Commit(opts *pebble.WriteOptions) error {
	return b.db.db.Write(b.db.wo, b.wb)
}

func (b rocksdbBatch) Set(key, value []byte, _ *pebble.WriteOptions) error {
	b.wb.Put(key, value)

	return nil
}

func (b rocksdbBatch) Delete(key []byte, _ *pebble.WriteOptions) error {
	b.wb.Delete(key)

	return nil
}

func (b rocksdbBatch) LogData(data []byte, _ *pebble.WriteOptions) error {
	panic("rocksdbBatch.logData: unimplemented")
}
