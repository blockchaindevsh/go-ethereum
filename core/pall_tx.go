package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gansidui/priority_queue"
	"sync"
)

type pallTxManager struct {
	block *types.Block
	txLen int
	bc    *BlockChain

	mubase      sync.RWMutex
	baseStateDB *state.StateDB

	mergedReceipts map[int]*types.Receipt
	mergedRW       map[int]map[common.Address]bool
	ch             chan struct{}
	mergedNumber   int

	muTx    sync.RWMutex
	txQueue *priority_queue.PriorityQueue

	receiptQueue []*ReceiptWithIndex

	gp *GasPool
}

type ReceiptWithIndex struct {
	st      *state.StateDB
	txIndex int
	receipt *types.Receipt
}

type Index int

func (this Index) Less(other interface{}) bool {
	return this < other.(Index)
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManager {
	st.MergedIndex = -1
	txLen := len(block.Transactions())
	p := &pallTxManager{
		block:          block,
		txLen:          txLen,
		baseStateDB:    st,
		bc:             bc,
		mergedReceipts: make(map[int]*types.Receipt, 0),
		mergedRW:       make(map[int]map[common.Address]bool),
		ch:             make(chan struct{}, 1),
		mergedNumber:   -1,
		receiptQueue:   make([]*ReceiptWithIndex, txLen, txLen),
		txQueue:        priority_queue.New(),
		gp:             new(GasPool).AddGas(block.GasLimit()),
	}
	for index := 0; index < 4; index++ {
		go p.txLoop()
	}
	return p
}

func (p *pallTxManager) AddTxToQueue(txIndex int) {
	p.muTx.Lock()
	defer p.muTx.Unlock()
	p.txQueue.Push(Index(txIndex))
}

func (p *pallTxManager) GetTxFromQueue() int {
	p.muTx.Lock()
	defer p.muTx.Unlock()
	if p.txQueue.Len() == 0 {
		return -1
	}
	return int(p.txQueue.Pop().(Index))
}

func (p *pallTxManager) AddReceiptToQueue(re *ReceiptWithIndex) {
	p.receiptQueue[re.txIndex] = re

	startTxIndex := re.txIndex
	for p.mergedNumber+1 == startTxIndex && startTxIndex < p.txLen && p.receiptQueue[startTxIndex] != nil {
		p.handleReceipt(p.receiptQueue[startTxIndex])
		startTxIndex++
	}

	if p.Done() {
		p.ch <- struct{}{}
	}
}

func (p *pallTxManager) Done() bool {
	return p.mergedNumber+1 == p.txLen
}

func (p *pallTxManager) txLoop() {
	for {
		if p.Done() {
			return
		}
		tx := p.GetTxFromQueue()
		if tx != -1 {
			if !p.handleTx(tx) {
				p.AddTxToQueue(tx)
			}
		}
	}
}

func (p *pallTxManager) handleReceipt(rr *ReceiptWithIndex) {
	p.mubase.Lock()
	defer p.mubase.Unlock()

	if rr.st.CanMerge(p.mergedRW) {
		rr.st.Merge(p.baseStateDB)
		p.gp.SubGas(rr.receipt.GasUsed)
		p.mergedReceipts[rr.txIndex] = rr.receipt
		p.mergedRW[rr.txIndex] = rr.st.ThisTxRW
		p.mergedNumber = rr.txIndex
		fmt.Println("merge end", "blockNumber", p.block.NumberU64(), p.mergedNumber)
	} else {
		if rr.st.TxIndex() > p.baseStateDB.MergedIndex { // conflict
			p.AddTxToQueue(rr.txIndex)
		}
	}
}

func (p *pallTxManager) handleTx(txIndex int) bool {
	tx := p.block.Transactions()[txIndex]
	p.mubase.Lock()
	if txIndex <= p.baseStateDB.MergedIndex { //already merged,abort
		p.mubase.Unlock()
		return true
	}
	st := p.baseStateDB.Copy()
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)

	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(p.gp.Gas()), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		fmt.Println("apply tx err", err, "blockNumber", p.block.NumberU64(), txIndex)
		return false
	}
	p.AddReceiptToQueue(&ReceiptWithIndex{
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
	return true

}

func (p *pallTxManager) GetReceiptsAndLogs() (types.Receipts, []*types.Log, uint64) {
	receipts := make(types.Receipts, 0)
	logs := make([]*types.Log, 0)
	txLen := len(p.block.Transactions())
	CumulativeGasUsed := uint64(0)
	for index := 0; index < txLen; index++ {
		CumulativeGasUsed = CumulativeGasUsed + p.mergedReceipts[index].GasUsed
		p.mergedReceipts[index].CumulativeGasUsed = CumulativeGasUsed
		p.mergedReceipts[index].Bloom = types.CreateBloom(types.Receipts{p.mergedReceipts[index]})
		receipts = append(receipts, p.mergedReceipts[index])
		logs = append(logs, p.mergedReceipts[index].Logs...)
	}
	return receipts, logs, CumulativeGasUsed
}
