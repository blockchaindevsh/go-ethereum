package core

import (
	"container/heap"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"sync"
)

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var (
	rootAddr = make(map[common.Address]common.Address, 0)
)

func Find(x common.Address) common.Address {
	if rootAddr[x] != x {
		rootAddr[x] = Find(rootAddr[x])
	}
	return rootAddr[x]
}

func Union(x common.Address, y *common.Address) {
	if _, ok := rootAddr[x]; !ok {
		rootAddr[x] = x
	}
	if y == nil {
		return
	}
	if _, ok := rootAddr[*y]; !ok {
		rootAddr[*y] = *y
	}
	fx := Find(x)
	fy := Find(*y)
	if fx != fy {
		rootAddr[fy] = fx
	}
}

func grouping(from []common.Address, to []*common.Address) map[int][]int {
	rootAddr = make(map[common.Address]common.Address, 0)
	for index, sender := range from {
		Union(sender, to[index])
	}

	groupList := make(map[int][]int, 0)
	mpGroup := make(map[common.Address]int, 0) // key:rootNode; value groupID

	for index, sender := range from {
		fa := Find(sender)
		groupID, ok := mpGroup[fa]
		if !ok {
			groupID = len(groupList)
			mpGroup[fa] = groupID

		}
		groupList[groupID] = append(groupList[groupID], index)
	}
	return groupList

}

type txSortManager struct {
	mu   sync.Mutex
	heap *IntHeap

	groupLen           int
	nextTxIndexInGroup map[int]int
}

func NewSortTxManager(from []common.Address, to []*common.Address) *txSortManager {
	groupList := grouping(from, to)

	nextTxIndexInGroup := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			nextTxIndexInGroup[list[index]] = list[index+1]
		}
	}

	heapList := make(IntHeap, 0)
	for _, v := range groupList {
		heapList = append(heapList, v[0])
	}
	heap.Init(&heapList)

	return &txSortManager{
		heap:               &heapList,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
	}
}

func (s *txSortManager) pushNext(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		heap.Push(s.heap, nextTxIndex)
	}
}

func (s *txSortManager) pop() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return -1
	}
	return heap.Pop(s.heap).(int)
}

type pallTxManager struct {
	block *types.Block
	txLen int
	bc    *BlockChain

	mu             sync.Mutex
	baseStateDB    *state.StateDB
	mergedReceipts []*types.Receipt
	ch             chan struct{}
	ended          bool

	txSortManger *txSortManager

	txQueue   chan int
	txResults []*txResult
	gp        uint64
}

type txResult struct {
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

		txResults: make([]*txResult, txLen, txLen),
		gp:        block.GasLimit(),
	}

	signer := types.MakeSigner(bc.chainConfig, block.Number())
	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)
	for _, tx := range block.Transactions() {
		sender, _ := types.Sender(signer, tx)
		fromList = append(fromList, sender)
		toList = append(toList, tx.To())
	}
	p.txSortManger = NewSortTxManager(fromList, toList)

	thread := p.txSortManger.groupLen
	if thread > 8 {
		thread = 8
	}

	p.txQueue = make(chan int, thread)
	for index := 0; index < thread; index++ {
		go p.txLoop()
	}

	for index := 0; index < thread; index++ {
		p.AddTxToQueue(p.txSortManger.pop())
	}
	fmt.Println("NNNNNNNNNNNNNN", block.NumberU64())
	return p
}

func (p *pallTxManager) AddTxToQueue(txIndex int) {
	if txIndex == -1 {
		return
	}
	p.txQueue <- txIndex
}

func (p *pallTxManager) GetTxFromQueue() (int, bool) {
	data, ok := <-p.txQueue
	return data, ok == false
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) {
	p.txResults[re.txIndex] = re
	startTxIndex := re.txIndex

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ended {
		return
	}

	for p.baseStateDB.MergedIndex+1 == startTxIndex && startTxIndex < p.txLen && p.txResults[startTxIndex] != nil {
		p.handleReceipt(p.txResults[startTxIndex])
		startTxIndex++
	}

	if p.baseStateDB.MergedIndex+1 == p.txLen {
		p.baseStateDB.FinalUpdateObjs(p.block.Coinbase())
		p.ch <- struct{}{}
		p.ended = true
	}
}

func (p *pallTxManager) txLoop() {
	for {
		txIndex, isClosed := p.GetTxFromQueue()
		if isClosed {
			return
		}

		if !p.handleTx(txIndex) && !p.ended {
			p.AddTxToQueue(txIndex)
		} else {
			p.AddTxToQueue(p.txSortManger.pop())
		}
	}
}

func (p *pallTxManager) handleReceipt(rr *txResult) {
	if rr.receipt != nil && !rr.st.Conflict(p.block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), p.block.Transactions()[rr.txIndex].GasPrice())
		rr.st.Merge(p.baseStateDB, p.block.Coinbase(), txFee)
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.txIndex] = rr.receipt

		p.txSortManger.pushNext(rr.txIndex)
		return
	}

	p.txResults[rr.txIndex] = nil
	p.AddTxToQueue(rr.txIndex)
}

func (p *pallTxManager) handleTx(txIndex int) bool {
	tx := p.block.Transactions()[txIndex]
	st, _ := state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)

	p.mu.Lock()
	st.MergedSts = p.baseStateDB.MergedSts
	st.MergedIndex = p.baseStateDB.MergedIndex
	gas := p.gp
	p.mu.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)

	if err != nil && st.MergedIndex+1 == txIndex {
		fmt.Println("---apply tx err---", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex)
		panic("should panic")
	}

	p.AddReceiptToQueue(&txResult{
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
	return true

}

func (p *pallTxManager) GetReceiptsAndLogs() (types.Receipts, []*types.Log, uint64) {
	logs := make([]*types.Log, 0)
	cumulativeGasUsed := uint64(0)

	for index := 0; index < p.txLen; index++ {
		cumulativeGasUsed = cumulativeGasUsed + p.mergedReceipts[index].GasUsed
		p.mergedReceipts[index].CumulativeGasUsed = cumulativeGasUsed
		logs = append(logs, p.mergedReceipts[index].Logs...)
	}
	return p.mergedReceipts, logs, cumulativeGasUsed
}
