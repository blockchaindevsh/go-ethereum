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
	st.MergedIndex = -1
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
	//fmt.Println("AddTxxxx", tx.Hash().String(), txIndex)
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
		if tx != nil {
			if !p.handleTx(tx.tx, tx.txIndex) {
				p.AddTxToQueue(tx.tx, tx.txIndex)
			}
		}
		time.Sleep(1 * time.Second) //TODO need delete
	}
}

func (p *pallTxManage) mergeLoop() {
	for {
		rr := p.GetReceiptFromQueue()
		if rr == nil {
			continue
		}

		p.mubase.Lock()
		if p.baseStateDB.MergedIndex+1 != rr.txIndex {
			if p.baseStateDB.MergedIndex < rr.txIndex {
				p.AddReceiptToQueue(rr)
			}
			p.mubase.Unlock()
			continue
		}

		if rr.st.CanMerge(p.baseStateDB) {
			rr.st.Merge(p.baseStateDB)

			p.gp.SubGas(rr.receipt.GasUsed)
			p.receipts[rr.txIndex] = rr.receipt

			fmt.Println("end to merge", "blockNumber", p.block.NumberU64(), "txIndex", rr.st.TxIndex(), "currBase", rr.st.MergedIndex, "baseMergedNumber", p.baseStateDB.MergedIndex, rr.st.GetNonce(common.HexToAddress("0x54dAeb3E8a6BBC797E4aD2b0339f134b186e4637")), p.baseStateDB.GetNonce(common.HexToAddress("0x54dAeb3E8a6BBC797E4aD2b0339f134b186e4637")), rr.st.GetNonce(common.HexToAddress("0xF04842b2B7e246B4b3A95AE411175183DE614E07")), p.baseStateDB.GetNonce(common.HexToAddress("0xF04842b2B7e246B4b3A95AE411175183DE614E07")))
		} else {
			if rr.st.TxIndex() > p.baseStateDB.MergedIndex { // 产生冲突
				fmt.Println("again add to queue", rr.st.TxIndex(), p.baseStateDB.MergedIndex)
				p.AddTxToQueue(rr.tx, rr.txIndex)
			}
		}
		if p.baseStateDB.MergedIndex == len(p.block.Transactions())-1 {
			p.markEnd()
			p.mubase.Unlock()
			return
		}
		p.mubase.Unlock()
	}
}

func (p *pallTxManage) handleTx(tx *types.Transaction, txIndex int) bool {
	p.mubase.Lock()
	if p.InCurrTask(txIndex, p.baseStateDB.MergedIndex) { //TODO delete task
		fmt.Println("IIIIIIIIIIIIIInCurrTask", txIndex, tx.Hash().String(), p.baseStateDB.MergedIndex)
		p.mubase.Unlock()
		return true
	}
	if txIndex <= p.baseStateDB.MergedIndex { //已经merge过的，直接丢弃
		p.mubase.Unlock()
		return true
	}
	st := p.baseStateDB.Copy()
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)

	p.SetCurrTask(txIndex, st.MergedIndex)
	defer p.DeleteCurrTask(txIndex, st.MergedIndex)

	//fmt.Println("??????????????????????????-handle tx", tx.Hash().String(), txIndex, st.MergedIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(p.gp.Gas()), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	fmt.Println("??????????????????????????-handle tx", tx.Hash().String(), txIndex, st.MergedIndex, err)
	if err != nil {
		//p.AddTxToQueue(tx, txIndex)
		return false
	}
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
	receipts := make(types.Receipts, 0)
	logs := make([]*types.Log, 0)
	txLen := len(p.block.Transactions())
	CumulativeGasUsed := uint64(0)
	for index := 0; index < txLen; index++ {
		CumulativeGasUsed = CumulativeGasUsed + p.receipts[index].GasUsed
		p.receipts[index].CumulativeGasUsed = CumulativeGasUsed
		p.receipts[index].Bloom = types.CreateBloom(types.Receipts{p.receipts[index]})
		receipts = append(receipts, p.receipts[index])
		logs = append(logs, p.receipts[index].Logs...)
	}
	return receipts, logs, CumulativeGasUsed
}
