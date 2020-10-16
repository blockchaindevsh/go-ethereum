package trie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
)

type fastDB struct {
	db    *Database
	cache map[string][]byte
	batch ethdb.Batch
}

func NewFastDB(db *Database) *fastDB {
	dd := db.diskdb.NewBatch()
	return &fastDB{
		db:    db,
		batch: dd,
		cache: make(map[string][]byte),
	}
}

func (f *fastDB) GetKey(key []byte) []byte {
	panic("no need to implement")
}
func (f *fastDB) TryGet(key []byte) ([]byte, error) {
	data, _ := f.db.diskdb.Get(key)
	return data, nil
}
func (f *fastDB) TryUpdate(key, value []byte) error {
	f.cache[string(key)] = value
	return f.batch.Put(key, value)
}
func (f *fastDB) TryDelete(key []byte) error {
	delete(f.cache, string(key))
	return f.batch.Delete(key)
}
func (f *fastDB) Hash() common.Hash {
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

func (f *fastDB) Commit(onleaf LeafCallback) (common.Hash, error) {
	err := f.batch.Write()
	hash := f.Hash()
	f.cache = make(map[string][]byte)
	return hash, err
}
func (f *fastDB) NodeIterator(startKey []byte) NodeIterator {
	panic("fastdb NodeIterator not implement")
}
func (f *fastDB) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error {
	panic("fastdb Prove not implement")
}
