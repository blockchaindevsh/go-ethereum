package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"sync"
)

type pallTxManager struct {
	block *types.Block
	txLen int
	bc    *BlockChain

	mubase         sync.RWMutex
	baseStateDB    *state.StateDB
	mergedReceipts []*types.Receipt
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
	txLen := len(block.Transactions())
	p := &pallTxManager{
		block: block,
		txLen: txLen,
		bc:    bc,

		baseStateDB:    st,
		mergedReceipts: make([]*types.Receipt, txLen, txLen),
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
	//if common.PrintExtraLog {
	fmt.Println("PALL TX READY", block.Number(), p.groupList)
	//}

	for index := 0; index < len(p.groupList); index++ {
		p.AddTxToQueue(p.groupList[index][0])
	}
	return p
}

var (
	fatherMp = make(map[common.Address]common.Address, 0)
	tryCnt   = uint64(0) //TODO delete
)

func Find(x common.Address) common.Address {
	if fatherMp[x] != x {
		fatherMp[x] = Find(fatherMp[x])
	}
	return fatherMp[x]
}

func Union(x common.Address, y *common.Address) {
	if _, ok := fatherMp[x]; !ok {
		fatherMp[x] = x
	}
	if y == nil {
		return
	}
	if _, ok := fatherMp[*y]; !ok {
		fatherMp[*y] = *y
	}
	fx := Find(x)
	fy := Find(*y)
	if fx != fy {
		fatherMp[fy] = fx
	}
}

func CalGroup(from []common.Address, to []*common.Address) (map[int][]int, map[int]int) {
	fatherMp = make(map[common.Address]common.Address, 0) //https://etherscan.io/txs?block=342007
	tryCnt = 0
	for index, sender := range from {
		Union(sender, to[index])
	}

	groupList := make(map[int][]int, 0)
	mpGroup := make(map[common.Address]int, 0) // key:rootNode; value groupID

	txIndexToGroupID := make(map[int]int, 0)
	for index, sender := range from {
		fa := Find(sender)
		groupID, ok := mpGroup[fa]
		if !ok {
			groupID = len(groupList)
			mpGroup[fa] = groupID

		}
		groupList[groupID] = append(groupList[groupID], index)
		txIndexToGroupID[index] = groupID

	}
	return groupList, txIndexToGroupID

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
		//fmt.Println("BBBBBBBBBBBBBB", p.block.Number())
		p.baseStateDB.FinalUpdateObjs(p.block.Coinbase())
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
	txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), p.block.Transactions()[rr.txIndex].GasPrice())
	if rr.st.TryMerge(p.baseStateDB, p.block.Coinbase(), txFee) {

		p.baseStateDB.MergedIndex = rr.txIndex
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.txIndex] = rr.receipt

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
	st.MergedSts = p.baseStateDB.MergedSts
	st.MergedIndex = p.baseStateDB.MergedIndex
	gas := p.gp
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	//fmt.Println("RRRRRRRRRRRRRReeeeeeee", st.MergedIndex, txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	//fmt.Println("RRRRRRRRRRRRRReeeeeeee", st.MergedIndex, txIndex, receipt.GasUsed)
	if err != nil {
		fmt.Println("---apply tx err---", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex, "groupList", p.groupList)
		tryCnt++
		if tryCnt > 20 {
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
