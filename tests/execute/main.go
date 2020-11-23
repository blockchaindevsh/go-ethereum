package main

import (
	"fmt"
	"sync"
)

type addr int
type hash int

type tx struct {
	txIndex int
	read    []addr
	write   []addr
}

type cache struct {
	mu     sync.Mutex
	origin map[addr]map[hash]hash

	pending   map[addr]map[hash]hash
	lastWrite map[addr]int
}

func makeCache() *cache {
	return &cache{
		mu:        sync.Mutex{},
		origin:    make(map[addr]map[hash]hash),
		pending:   make(map[addr]map[hash]hash),
		lastWrite: make(map[addr]int),
	}
}

func (c *cache) getKey(addr addr, key hash) (hash, bool) {
	data, ok := c.origin[addr]
	if !ok {
		return 0, false
	}
	value, exist := data[key]
	return value, exist
}
func (c *cache) setKey(addr addr, key hash, value hash) {
	_, ok := c.origin[addr]
	if !ok {
		c.origin[addr] = make(map[hash]hash, 0)
	}
	c.origin[addr][key] = value
}

type statedb struct {
	mergedIndex int
	dirty       map[addr]map[hash]hash
}

func newStateDB(c *cache) *statedb {
	return &statedb{
		mergedIndex: -1,
		dirty:       make(map[addr]map[hash]hash),
	}
}

type executeManager struct {
	baSt  *statedb
	cache *cache
}

func newManager(st *statedb) *executeManager {
	return &executeManager{
		baSt:  st,
		cache: makeCache(),
	}
}

func (e *executeManager) createStatedb() *statedb {
	return &statedb{
		mergedIndex: e.baSt.mergedIndex,
		dirty:       make(map[addr]map[hash]hash),
	}
}
func (e *executeManager) executeTx(st *statedb, tx *tx) (*statedb, map[addr]bool) {
	rwSet := make(map[addr]bool, 0)
	for _, addr := range tx.read {
		rwSet[addr] = false
	}

	for _, addr := range tx.write {
		if _, ok := st.dirty[addr]; !ok {
			st.dirty[addr] = make(map[hash]hash, 0)
		}
		st.dirty[addr][1] = 1
		rwSet[addr] = true
	}
	return st, rwSet
}

func (e *executeManager) mergeTx(txIndex int, rw map[addr]bool, st *statedb) bool {
	for addr, _ := range rw {
		if lastIndex, ok := e.cache.lastWrite[addr]; ok && lastIndex > st.mergedIndex {
			fmt.Printf("存在冲突 addr=%v 上次修改点=%v 本次执行基于点=%v\n", addr, lastIndex, st.mergedIndex)
			return false
		}

	}

	for addr, dir := range st.dirty {
		for key, value := range dir {
			if _, ok := e.cache.pending[addr]; !ok {
				e.cache.pending[addr] = make(map[hash]hash)
			}
			e.cache.pending[addr][key] = value
		}
		e.cache.lastWrite[addr] = txIndex
	}
	e.baSt.mergedIndex = txIndex
	return true
}

func genTx(txIndex int, read []addr, write []addr) *tx {
	return &tx{
		txIndex: txIndex,
		read:    read,
		write:   write,
	}

}

func main() {
	m := newManager(&statedb{
		mergedIndex: -1,
		dirty:       make(map[addr]map[hash]hash, 0),
	})

	txs := make([]*tx, 2, 2)
	txs[0] = genTx(0, []addr{2, 4}, []addr{1, 2, 3})
	txs[1] = genTx(1, []addr{2}, []addr{4})

	for _, v := range txs {
		st := m.createStatedb()
		st, rw := m.executeTx(st, v)
		fmt.Printf("执行完毕 基于%v 执行 %v\n", st.mergedIndex, v.txIndex)
		status := m.mergeTx(v.txIndex, rw, st)
		fmt.Printf("合并完毕 状态=%v lastWrite=%v\n", status, m.cache.lastWrite)
	}

	fmt.Println("\n")

	m = newManager(&statedb{
		mergedIndex: -1,
		dirty:       make(map[addr]map[hash]hash, 0),
	})

	txs = make([]*tx, 3, 3)
	txs[0] = genTx(0, []addr{2, 4}, []addr{1, 2, 3})
	txs[1] = genTx(1, []addr{2}, []addr{4})
	txs[2] = genTx(1, []addr{2}, []addr{4})

	for k, v := range txs {
		st := m.createStatedb()
		if k == 1 {
			st.mergedIndex = -1
		}
		st, rw := m.executeTx(st, v)
		fmt.Printf("执行完毕 基于%v 执行 %v\n", st.mergedIndex, v.txIndex)
		status := m.mergeTx(v.txIndex, rw, st)
		fmt.Printf("合并完毕 状态=%v lastWrite=%v\n", status, m.cache.lastWrite)
	}
}
