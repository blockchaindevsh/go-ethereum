package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"sync"
	"time"
)

type pallTxManager struct {
	block *types.Block
	txLen int
	bc    *BlockChain

	mubase         sync.RWMutex
	baseStateDB    *state.StateDB
	mergedReceipts []*types.Receipt
	mergedRW       map[int]map[common.Address]bool
	ch             chan struct{}
	ended          bool

	lastHandleInGroup map[int]int
	txIndexToGroupID  map[int]int
	groupList         map[int][]int

	txQueue      chan int
	receiptQueue []*ReceiptWithIndex
	gp           uint64
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
		block: block,
		txLen: txLen,
		bc:    bc,

		baseStateDB:    st,
		mergedReceipts: make([]*types.Receipt, txLen, txLen),
		mergedRW:       make(map[int]map[common.Address]bool),
		ch:             make(chan struct{}, 1),

		lastHandleInGroup: make(map[int]int),
		txIndexToGroupID:  make(map[int]int, 0),
		groupList:         make(map[int][]int, 0),

		txQueue:      make(chan int, txLen),
		receiptQueue: make([]*ReceiptWithIndex, txLen, txLen),
		gp:           block.GasLimit(),
	}

	signer := types.MakeSigner(bc.chainConfig, block.Number())

	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)
	for _, tx := range block.Transactions() { // TODO 有漏洞，未完全分组？？？
		sender, _ := types.Sender(signer, tx)
		fromList = append(fromList, sender)
		toList = append(toList, tx.To())
	}
	p.groupList, p.txIndexToGroupID = CalGroup(fromList, toList)

	for index := 0; index < 8; index++ {
		go p.txLoop()
	}
	if common.PrintExtraLog {
		fmt.Println("PALL TX READY", block.Number(), p.groupList)
	}

	for index := 0; index < len(p.groupList); index++ {
		p.AddTxToQueue(p.groupList[index][0])
	}
	return p
}

var (
	father = make(map[common.Address]common.Address, 0)
	tryCnt = uint64(0) //TODO delete
)

func Find(x common.Address) common.Address {
	if father[x] != x {
		father[x] = Find(father[x])
	}
	return father[x]
}
func Union(x common.Address, y *common.Address) {
	if _, ok := father[x]; !ok {
		father[x] = x
	}
	if y == nil {
		return
	}
	if _, ok := father[*y]; !ok {
		father[*y] = *y
	}
	fx := Find(x)
	fy := Find(*y)
	if fx != fy {
		father[fy] = fx
	}
}

func CalGroup(from []common.Address, to []*common.Address) (map[int][]int, map[int]int) {
	father = make(map[common.Address]common.Address, 0)
	tryCnt = 0
	//https://etherscan.io/txs?block=342007
	for index, sender := range from {
		Union(sender, to[index])
	}

	res := make(map[int][]int, 0)
	mpGroup := make(map[common.Address]int, 0) //key 最终的根节点;value 组id

	txIndexToGroupID := make(map[int]int, 0)
	for index, sender := range from {
		fa := Find(sender)
		groupID, ok := mpGroup[fa]
		if !ok {
			groupID = len(res)
			mpGroup[fa] = groupID

		}
		res[groupID] = append(res[groupID], index)
		txIndexToGroupID[index] = groupID

	}
	return res, txIndexToGroupID

}

func (p *pallTxManager) calGroup(mp map[common.Address]int, from common.Address, to *common.Address) int {
	groupID := len(p.groupList)

	if to != nil {
		if data, ok := mp[*to]; ok {
			groupID = data
		}
	}
	if data, ok := mp[from]; ok {
		groupID = data
	}

	mp[from] = groupID
	if to != nil {
		mp[*to] = groupID
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

	if p.ended {
		return
	}

	for p.baseStateDB.MergedIndex+1 == startTxIndex && startTxIndex < p.txLen && p.receiptQueue[startTxIndex] != nil {
		p.handleReceipt(p.receiptQueue[startTxIndex])
		startTxIndex++
	}

	if p.baseStateDB.MergedIndex+1 == p.txLen {
		p.baseStateDB.FinalUpdateObjs(p.mergedRW, p.block.Coinbase())
		p.ch <- struct{}{}
		p.ended = true
	}
}

func (p *pallTxManager) txLoop() {
	for {
		tx, isClosed := p.GetTxFromQueue()
		if isClosed {
			return
		}
		if !p.handleTx(tx) && !p.ended {
			p.AddTxToQueue(tx)
		}
	}
}

func (p *pallTxManager) handleReceipt(rr *ReceiptWithIndex) {
	if rr.st.CanMerge(p.mergedRW, p.block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), p.block.Transactions()[rr.txIndex].GasPrice())
		//fmt.Println("ready to merge", rr.txIndex, rr.receipt.GasUsed)
		rr.st.Merge(p.baseStateDB, p.block.Coinbase(), txFee)

		p.baseStateDB.MergedIndex = rr.txIndex
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.txIndex] = rr.receipt
		p.mergedRW[rr.txIndex] = rr.st.RWSet

		groupID := p.txIndexToGroupID[rr.txIndex]
		p.lastHandleInGroup[groupID]++
		if p.lastHandleInGroup[groupID] < len(p.groupList[groupID]) {
			p.AddTxToQueue(p.groupList[groupID][p.lastHandleInGroup[groupID]])
		}
		return
	}

	p.receiptQueue[rr.txIndex] = nil
	p.AddTxToQueue(rr.txIndex)
}

func (p *pallTxManager) handleTx(txIndex int) bool {
	tx := p.block.Transactions()[txIndex]

	st, err := state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
	if err != nil {
		panic(err)
	}

	p.mubase.Lock()
	if txIndex <= p.baseStateDB.MergedIndex || p.receiptQueue[txIndex] != nil || p.ended { //delete?
		panic("mei bi yao")
	}

	st.MergedSts = p.baseStateDB.MergedSts
	st.MergedIndex = p.baseStateDB.MergedIndex
	gas := p.gp
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		fmt.Println("---apply tx err---", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex, "groupList", p.groupList)
		time.Sleep(5 * time.Second)
		tryCnt++
		if tryCnt > 1000 {
			panic("sb")
		}
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
	logs := make([]*types.Log, 0)
	CumulativeGasUsed := uint64(0)

	for index := 0; index < p.txLen; index++ {
		CumulativeGasUsed = CumulativeGasUsed + p.mergedReceipts[index].GasUsed
		p.mergedReceipts[index].CumulativeGasUsed = CumulativeGasUsed
		logs = append(logs, p.mergedReceipts[index].Logs...)
	}
	return p.mergedReceipts, logs, CumulativeGasUsed
}
