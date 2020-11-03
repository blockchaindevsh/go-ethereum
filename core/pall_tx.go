package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"sync"
)

type pallTxManager struct {
	block *types.Block
	txLen int
	bc    *BlockChain

	mubase         sync.RWMutex
	baseStateDB    *state.StateDB
	mergedReceipts map[int]*types.Receipt
	mergedRW       map[int]map[common.Address]bool
	ch             chan struct{}
	mergedNumber   int

	lastHandleInGroup map[int]int

	txIndexToGroupID map[int]int
	addressToGroupID map[common.Address]int
	groupList        map[int][]int // key sender ; value tx index List

	txQueue      chan int
	receiptQueue []*ReceiptWithIndex

	gp     *GasPool
	signer types.Signer
}

type ReceiptWithIndex struct {
	st      *state.StateDB
	txIndex int
	receipt *types.Receipt
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManager {
	st.MergedIndex = -1
	txLen := len(block.Transactions())
	p := &pallTxManager{
		block:             block,
		txLen:             txLen,
		baseStateDB:       st,
		bc:                bc,
		mergedReceipts:    make(map[int]*types.Receipt, 0),
		mergedRW:          make(map[int]map[common.Address]bool),
		ch:                make(chan struct{}, 1),
		txQueue:           make(chan int, txLen),
		txIndexToGroupID:  make(map[int]int, 0),
		addressToGroupID:  make(map[common.Address]int, 0),
		lastHandleInGroup: make(map[int]int),

		mergedNumber: -1,
		groupList:    make(map[int][]int, 0),
		receiptQueue: make([]*ReceiptWithIndex, txLen, txLen),

		gp:     new(GasPool).AddGas(block.GasLimit()),
		signer: types.MakeSigner(bc.chainConfig, block.Number()),
	}

	for k, v := range block.Transactions() {
		sender, _ := types.Sender(p.signer, v)

		groupID := p.calGroup(sender, v.To())
		if block.NumberU64() == 116525 {
			fmt.Println("txIndex", k, groupID)
		}

		p.groupList[groupID] = append(p.groupList[groupID], k)
		p.txIndexToGroupID[k] = groupID
	}

	if len(p.groupList) >= 8 {
		fmt.Println("block Print", p.block.Number(), "txs", len(p.block.Transactions()), "groupSize", len(p.groupList))
	}

	for index := 0; index < 8; index++ {
		go p.txLoop()
	}

	for index := 0; index < len(p.groupList); index++ {
		p.AddTxToQueue(p.groupList[index][0])
	}
	return p
}

func (p *pallTxManager) calGroup(from common.Address, to *common.Address) int {
	groupID := len(p.groupList)
	if data, ok := p.addressToGroupID[from]; ok {
		groupID = data
	}
	if to != nil {
		if data, ok := p.addressToGroupID[*to]; ok {
			groupID = data
		}
	}

	p.addressToGroupID[from] = groupID
	if p.block.NumberU64() == 116525 {
		fmt.Println("FFFFFFFFFFF", from.String(), groupID)
	}

	if to != nil {
		p.addressToGroupID[*to] = groupID
		p.addressToGroupID[from] = groupID
		if p.block.NumberU64() == 116525 {
			fmt.Println("TTTTTTTTTTTT", (*to).String(), groupID)
		}

	}
	return groupID
}

func (p *pallTxManager) AddTxToQueue(txIndex int) {
	p.txQueue <- txIndex
}

func (p *pallTxManager) GetTxFromQueue() (int, bool) {
	data, ok := <-p.txQueue
	return data, ok == false
}

func (p *pallTxManager) AddReceiptToQueue(re *ReceiptWithIndex) {
	p.receiptQueue[re.txIndex] = re
	startTxIndex := re.txIndex

	p.mubase.Lock()
	defer p.mubase.Unlock()

	if p.Done() {
		return
	}

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
		tx, isClosed := p.GetTxFromQueue()
		if isClosed {
			return
		}
		if !p.handleTx(tx) && !p.Done() {
			p.AddTxToQueue(tx)
		}
	}
}

func (p *pallTxManager) handleReceipt(rr *ReceiptWithIndex) {
	if rr.st.CanMerge(p.mergedRW) {
		rr.st.Merge(p.baseStateDB)
		p.gp.SubGas(rr.receipt.GasUsed)
		p.mergedReceipts[rr.txIndex] = rr.receipt
		p.mergedRW[rr.txIndex] = rr.st.ThisTxRW
		p.mergedNumber = rr.txIndex
		//fmt.Println("merge end", "blockNumber", p.block.NumberU64(), p.mergedNumber)

		groupID := p.txIndexToGroupID[rr.txIndex]
		p.lastHandleInGroup[groupID]++
		if p.lastHandleInGroup[groupID] < len(p.groupList[groupID]) {
			p.AddTxToQueue(p.groupList[groupID][p.lastHandleInGroup[groupID]])
		}

	} else {
		p.AddTxToQueue(rr.txIndex)
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
	gas := p.gp.Gas()
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)

	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		fmt.Println("apply tx err", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex, "groupList", p.groupList)
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

	CumulativeGasUsed := uint64(0)
	for index := 0; index < p.txLen; index++ {
		CumulativeGasUsed = CumulativeGasUsed + p.mergedReceipts[index].GasUsed
		p.mergedReceipts[index].CumulativeGasUsed = CumulativeGasUsed
		p.mergedReceipts[index].Bloom = types.CreateBloom(types.Receipts{p.mergedReceipts[index]})
		receipts = append(receipts, p.mergedReceipts[index])
		logs = append(logs, p.mergedReceipts[index].Logs...)
	}
	return receipts, logs, CumulativeGasUsed
}
