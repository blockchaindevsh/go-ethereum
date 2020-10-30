package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gansidui/priority_queue"
	"sync"
	"time"
)

type pallTxManage struct {
	block *types.Block
	bc    *BlockChain

	mubase      sync.RWMutex
	baseStateDB *state.StateDB
	receipts    map[int]*types.Receipt

	ch chan struct{}

	muTx    sync.RWMutex
	txQueue *priority_queue.PriorityQueue

	muRe         sync.RWMutex
	receiptQueue *priority_queue.PriorityQueue

	merged bool

	currTask map[int]map[int]struct{}
	muTask   sync.RWMutex

	gp *GasPool
}

type TxWithIndex struct {
	tx      *types.Transaction
	txIndex int
}

type ReceiptWithIndex struct {
	tx      *types.Transaction
	st      *state.StateDB
	txIndex int
	receipt *types.Receipt
}

func (this *TxWithIndex) Less(other interface{}) bool {
	return this.txIndex < other.(*TxWithIndex).txIndex
}

func (this *ReceiptWithIndex) Less(other interface{}) bool {
	return this.txIndex < other.(*ReceiptWithIndex).txIndex
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManage {
	st.CurrMergedNumber = -1
	p := &pallTxManage{
		block:        block,
		baseStateDB:  st,
		bc:           bc,
		receipts:     make(map[int]*types.Receipt, 0),
		ch:           make(chan struct{}, 1),
		txQueue:      priority_queue.New(),
		receiptQueue: priority_queue.New(),
		currTask:     make(map[int]map[int]struct{}, 0),
		gp:           new(GasPool).AddGas(block.GasLimit()),
	}
	for index := 0; index < 4; index++ {
		go p.txLoop()
	}
	go p.mergeLoop()
	return p
}

func (p *pallTxManage) SetCurrTask(txIndex int, baseMergedIndex int) {
	p.muTask.Lock()
	defer p.muTask.Unlock()

	if _, ok := p.currTask[txIndex]; !ok {
		p.currTask[txIndex] = make(map[int]struct{})
	}
	p.currTask[txIndex][baseMergedIndex] = struct{}{}
}
func (p *pallTxManage) DeleteCurrTask(txIndex int, baseMergedIndex int) {
	p.muTask.Lock()
	defer p.muTask.Unlock()

	if data, ok := p.currTask[txIndex]; ok {
		delete(data, baseMergedIndex)
	}
}

func (p *pallTxManage) InCurrTask(txIndex int, baseIndex int) bool {
	p.muTask.RLock()
	defer p.muTask.RUnlock()

	if data, ok := p.currTask[txIndex]; ok {
		_, ok1 := data[baseIndex]
		return ok1
	}
	return false
}

func (p *pallTxManage) AddTxToQueue(tx *types.Transaction, txIndex int) {
	p.muTx.Lock()
	defer p.muTx.Unlock()
	p.txQueue.Push(&TxWithIndex{tx: tx, txIndex: txIndex})
}
func (p *pallTxManage) GetTxFromQueue() *TxWithIndex {
	p.muTx.Lock()
	defer p.muTx.Unlock()
	if p.txQueue.Len() == 0 {
		return nil
	}
	return p.txQueue.Pop().(*TxWithIndex)
}

func (p *pallTxManage) AddReceiptToQueue(re *ReceiptWithIndex) {
	p.muRe.Lock()
	defer p.muRe.Unlock()
	p.receiptQueue.Push(re)
}
func (p *pallTxManage) GetReceiptFromQueue() *ReceiptWithIndex {
	p.muRe.Lock()
	defer p.muRe.Unlock()
	if p.receiptQueue.Len() == 0 {
		return nil
	}
	return p.receiptQueue.Pop().(*ReceiptWithIndex)
}

func (p *pallTxManage) txLoop() {
	for {
		if p.merged {
			return
		}
		tx := p.GetTxFromQueue()
		fmt.Println("txloop  ", tx.txIndex, tx.tx.Hash().String())
		if tx != nil {
			if !p.handleTx(tx.tx, tx.txIndex) {
				p.AddTxToQueue(tx.tx, tx.txIndex)
			}
		}
		fmt.Println("txloop end", tx.txIndex, tx.tx.Hash().String())
		time.Sleep(1 * time.Second)
	}
}

func (p *pallTxManage) mergeLoop() {
	for {
		rr := p.GetReceiptFromQueue()
		if rr == nil {
			fmt.Println("RRRRRRRRRRRRRRR", "empty")
			time.Sleep(1 * time.Second)
			continue
		}

		p.mubase.Lock()

		fmt.Println("GetReFromQueue", p.block.NumberU64(), rr.txIndex, rr.st.CurrMergedNumber, p.baseStateDB.CurrMergedNumber)

		if rr.st.CanMerge(p.baseStateDB) {
			fmt.Println("ready to merge", "blockNumber", p.block.NumberU64(), "txIndex", rr.st.TxIndex(), "txHash", "baseMergedNumber", p.baseStateDB.CurrMergedNumber)
			rr.st.Merge(p.baseStateDB)

			p.gp.SubGas(rr.receipt.GasUsed)
			p.receipts[rr.txIndex] = rr.receipt

			fmt.Println("end to merge", "blockNumber", p.block.NumberU64(), "txIndex", rr.st.TxIndex(), "txHash", "baseMergedNumber", p.baseStateDB.CurrMergedNumber)
		} else {
			if rr.st.TxIndex() > p.baseStateDB.CurrMergedNumber {
				fmt.Println("again add to queue", rr.st.TxIndex(), p.baseStateDB.CurrMergedNumber)
				p.AddTxToQueue(rr.tx, rr.txIndex)
			}
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
	if txIndex <= p.baseStateDB.CurrMergedNumber {
		return true
	}
	st := p.baseStateDB.Copy()
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)

	p.SetCurrTask(txIndex, st.CurrMergedNumber)
	defer p.DeleteCurrTask(txIndex, st.CurrMergedNumber)

	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(p.gp.Gas()), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	fmt.Println("??????????????????????????-handle tx", tx.Hash().String(), txIndex, st.CurrMergedNumber, err)
	if err != nil {
		p.AddTxToQueue(tx, txIndex)
		return false
	}
	fmt.Println("AddReceiptttttt", txIndex, st.CurrMergedNumber)
	p.AddReceiptToQueue(&ReceiptWithIndex{
		tx:      tx,
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
	return true

}

func (p *pallTxManage) markEnd() {
	p.merged = true
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
