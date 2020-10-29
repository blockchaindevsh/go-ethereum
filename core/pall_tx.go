package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gansidui/priority_queue"
	"sync"
	"time"
)

type pallTxManage struct {
	block *types.Block
	bc    *BlockChain

	mubase          sync.RWMutex
	baseStateDB     *state.StateDB
	currentIndex    int
	currentReadMap  map[common.Address]struct{}
	currentWriteMap map[common.Address]struct{}
	gp              *GasPool
	receipts        map[int]*types.Receipt
	ch              chan struct{}
	txList          chan *TxWithIndex

	muTx    sync.RWMutex
	ququeTx *priority_queue.PriorityQueue

	muRe    sync.RWMutex
	ququeRe *priority_queue.PriorityQueue

	metged bool

	// key txIndex
	//value:
	//		key:currentMergedIndex
	currTask map[int]map[int]struct{}
	mu       sync.RWMutex
}

type TxWithIndex struct {
	tx      *types.Transaction
	txIndex int
}

type Re struct {
	st      *state.StateDB
	txIndex int
	receipt *types.Receipt
}

func (this *TxWithIndex) Less(other interface{}) bool {
	return this.txIndex < other.(*TxWithIndex).txIndex
}

func (this *Re) Less(other interface{}) bool {
	return this.txIndex < other.(*Re).txIndex
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManage {
	st.CurrMergedNumber = -1
	p := &pallTxManage{
		block:       block,
		baseStateDB: st,
		bc:          bc,
		gp:          new(GasPool).AddGas(block.GasLimit()),
		receipts:    make(map[int]*types.Receipt, 0),
		ch:          make(chan struct{}, 1),
		txList:      make(chan *TxWithIndex, 0),
		ququeTx:     priority_queue.New(),
		ququeRe:     priority_queue.New(),
		currTask:    make(map[int]map[int]struct{}, 0),
	}
	for index := 0; index < 4; index++ {
		go p.txLoop()
	}

	go p.mergeLoop()

	return p
}

func (p *pallTxManage) SetCurrTask(txInde int, baseMergedIndex int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.currTask[txInde]; !ok {
		p.currTask[txInde] = make(map[int]struct{})
	}
	p.currTask[txInde][baseMergedIndex] = struct{}{}
}
func (p *pallTxManage) DelteCurrTask(txInd int, baseMergedIndex int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if data, ok := p.currTask[txInd]; ok {
		delete(data, baseMergedIndex)
	}
}

func (p *pallTxManage) InCurrTask(txIndex int, baseMetgedIndex int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if data, ok := p.currTask[txIndex]; ok {
		_, ok1 := data[baseMetgedIndex]
		return ok1
	}
	return false
}

func (p *pallTxManage) AddTx(tx *types.Transaction, txIndex int) {
	p.muTx.Lock()
	defer p.muTx.Unlock()
	p.ququeTx.Push(&TxWithIndex{tx: tx, txIndex: txIndex})
}
func (p *pallTxManage) GetTx() *TxWithIndex {
	p.muTx.Lock()
	defer p.muTx.Unlock()
	if p.ququeTx.Len() == 0 {
		return nil
	}

	return p.ququeTx.Pop().(*TxWithIndex)
}

func (p *pallTxManage) AddRe(re *Re) {
	p.muRe.Lock()
	defer p.muRe.Unlock()
	p.ququeRe.Push(re)
}
func (p *pallTxManage) GetRe() *Re {
	p.muRe.Lock()
	defer p.muRe.Unlock()
	if p.ququeRe.Len() == 0 {
		return nil
	}
	return p.ququeRe.Pop().(*Re)
}

func (p *pallTxManage) GetMergedNumber() int {
	p.mubase.RLock()
	defer p.mubase.RUnlock()
	return p.baseStateDB.CurrMergedNumber
}
func (p *pallTxManage) txLoop() {
	if common.PrintData {
		defer fmt.Println("txLoop end", p.block.NumberU64())
	}

	for {
		tt := p.GetTx()
		if tt != nil {
			if !p.handleTx(tt.tx, tt.txIndex) {
				//fmt.Println("contimeeee", tt.txIndex, p.baseStateDB.CurrMergedNumber)
				if tt.txIndex > p.GetMergedNumber() { //baseStateDB 可能会更新
					p.AddTx(tt.tx, tt.txIndex)
				}
				continue
			}
		} else {
			time.Sleep(1 * time.Second)
		}
		if p.metged {
			//fmt.Println("already merged")
			return
		}
	}
}

func (p *pallTxManage) mergeLoop() {
	if common.PrintData {
		defer fmt.Println("mergeloop end", p.block.NumberU64())
	}

	for {
		rr := p.GetRe()
		if rr == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		p.mubase.Lock()
		if rr.st.CanMerge(p.baseStateDB) { //merged

			rr.st.Merge(p.baseStateDB)

		}
		if p.baseStateDB.CurrMergedNumber == len(p.block.Transactions())-1 {
			p.markEnd()
			p.mubase.Unlock()
			return
		}
		p.mubase.Unlock()

	}
}

func (p *pallTxManage) handleTx(tx *types.Transaction, txIndex int) bool {
	p.mubase.Lock()
	if p.InCurrTask(txIndex, p.baseStateDB.CurrMergedNumber) {
		p.mubase.Unlock()
		return false
	}
	st := p.baseStateDB.Copy()
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	p.SetCurrTask(txIndex, st.CurrMergedNumber)
	defer p.DelteCurrTask(txIndex, st.CurrMergedNumber)

	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, p.gp, st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		time.Sleep(1 * time.Second)
		p.AddTx(tx, txIndex)
		//panic(err)
		//fmt.Println("??????????????????????????-handle tx", tx.Hash().String(), txIndex, st.CurrMergedNumber, err)
		return false
	}

	p.AddRe(&Re{
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
	p.receipts[txIndex] = receipt
	return true

}

func (p *pallTxManage) markEnd() {
	p.metged = true
	p.ch <- struct{}{}
}

func (p *pallTxManage) GetReceiptsAndLogs() (types.Receipts, []*types.Log, uint64) {
	rs := make(types.Receipts, 0)
	logs := make([]*types.Log, 0)
	txLen := len(p.block.Transactions())
	all := uint64(0)
	for index := 0; index < txLen; index++ {
		all = all + p.receipts[index].GasUsed
		p.receipts[index].CumulativeGasUsed = all
		p.receipts[index].Bloom = types.CreateBloom(types.Receipts{p.receipts[index]})
		rs = append(rs, p.receipts[index])
		logs = append(logs, p.receipts[index].Logs...)
	}
	return rs, logs, all
}
