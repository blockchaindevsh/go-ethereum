package trie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
)

type FastDB struct {
	db    *Database
	cache map[string][]byte
	batch ethdb.Batch
}

func NewFastDB(db *Database) *FastDB {
	dd := db.diskdb.NewBatch()
	return &FastDB{
		db:    db,
		batch: dd,
		cache: make(map[string][]byte),
	}
}

func (f *FastDB) Copy() *FastDB {
	return &FastDB{
		db:    f.db,
		cache: make(map[string][]byte),
		batch: f.batch,
	}
}

func (f *FastDB) GetKey(key []byte) []byte {
	panic("no need to implement")
}
func (f *FastDB) TryGet(key []byte) ([]byte, error) {
	data, _ := f.db.diskdb.Get(key)
	return data, nil
}
func (f *FastDB) TryUpdate(key, value []byte) error {
	f.cache[string(key)] = value
	return f.batch.Put(key, value)
}
func (f *FastDB) TryDelete(key []byte) error {
	delete(f.cache, string(key))
	return f.batch.Delete(key)
}
func (f *FastDB) Hash() common.Hash {
	keyList := make([]string, 0, len(f.cache))
	for k, _ := range f.cache {
		keyList = append(keyList, k)
	}

	if len(f.cache) == 0 {
		return common.Hash{}
	}
	seed := make([]byte, 0)
	for _, k := range keyList {
		seed = append(seed, []byte(k)...)
		seed = append(seed, f.cache[k]...)
	}
	return common.BytesToHash(crypto.Keccak256(seed))
}

func (f *FastDB) Commit(onleaf LeafCallback) (common.Hash, error) {
	err := f.batch.Write()
	hash := f.Hash()
	f.cache = make(map[string][]byte)
	return hash, err
}
func (f *FastDB) NodeIterator(startKey []byte) NodeIterator {
	panic("fastdb NodeIterator not implement")
}
func (f *FastDB) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error {
	panic("fastdb Prove not implement")
}
