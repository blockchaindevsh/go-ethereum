package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gansidui/priority_queue"
	"sync"
	"time"
)

type pallTxManager struct {
	block *types.Block
	bc    *BlockChain

	mubase      sync.RWMutex
	baseStateDB *state.StateDB

	mergedReceipts map[int]*types.Receipt
	mergedRW       map[int]map[common.Address]bool
	ch             chan struct{}
	merged         bool

	muTx    sync.RWMutex
	txQueue *priority_queue.PriorityQueue

	muRe         sync.RWMutex
	receiptQueue *priority_queue.PriorityQueue

	gp *GasPool
}

type ReceiptWithIndex struct {
	st      *state.StateDB
	txIndex int
	receipt *types.Receipt
}

func (this *ReceiptWithIndex) Less(other interface{}) bool {
	return this.txIndex < other.(*ReceiptWithIndex).txIndex
}

type Index int

func (this Index) Less(other interface{}) bool {
	return this < other.(Index)
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManager {
	st.MergedIndex = -1
	p := &pallTxManager{
		block:          block,
		baseStateDB:    st,
		bc:             bc,
		mergedReceipts: make(map[int]*types.Receipt, 0),
		mergedRW:       make(map[int]map[common.Address]bool),
		ch:             make(chan struct{}, 1),
		txQueue:        priority_queue.New(),
		receiptQueue:   priority_queue.New(),
		gp:             new(GasPool).AddGas(block.GasLimit()),
	}
	for index := 0; index < 4; index++ {
		go p.txLoop()
	}
	go p.mergeLoop()
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
	p.muRe.Lock()
	defer p.muRe.Unlock()

	p.receiptQueue.Push(re)
}

func (p *pallTxManager) GetReceiptFromQueue() *ReceiptWithIndex {
	p.muRe.Lock()
	defer p.muRe.Unlock()
	if p.receiptQueue.Len() == 0 {
		return nil
	}
	return p.receiptQueue.Pop().(*ReceiptWithIndex)
}

func (p *pallTxManager) txLoop() {
	for {
		if p.merged {
			return
		}
		tx := p.GetTxFromQueue()
		if tx != -1 {
			if !p.handleTx(tx) {
				p.AddTxToQueue(tx)
			}
		}
		time.Sleep(1 * time.Second) //TODO need delete
	}
}

func (p *pallTxManager) mergeLoop() {
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

		if rr.st.CanMerge(p.mergedRW) {
			rr.st.Merge(p.baseStateDB)
			p.gp.SubGas(rr.receipt.GasUsed)
			p.mergedReceipts[rr.txIndex] = rr.receipt
			p.mergedRW[rr.txIndex] = rr.st.ThisTxRW
			//fmt.Println("end to merge", "blockNumber", p.block.NumberU64(), "txIndex", rr.st.TxIndex(), "currBase", rr.st.MergedIndex, "baseMergedNumber", p.baseStateDB.MergedIndex, rr.st.GetNonce(common.HexToAddress("0x54dAeb3E8a6BBC797E4aD2b0339f134b186e4637")), p.baseStateDB.GetNonce(common.HexToAddress("0x54dAeb3E8a6BBC797E4aD2b0339f134b186e4637")), rr.st.GetNonce(common.HexToAddress("0xF04842b2B7e246B4b3A95AE411175183DE614E07")), p.baseStateDB.GetNonce(common.HexToAddress("0xF04842b2B7e246B4b3A95AE411175183DE614E07")))
		} else {
			if rr.st.TxIndex() > p.baseStateDB.MergedIndex { // conflict
				p.AddTxToQueue(rr.txIndex)
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
		//fmt.Println("??????????????????????????-handle tx", tx.Hash().String(), txIndex, st.MergedIndex, err)
		return false
	}
	p.AddReceiptToQueue(&ReceiptWithIndex{
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
	return true

}

func (p *pallTxManager) markEnd() {
	p.merged = true
	p.ch <- struct{}{}
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
