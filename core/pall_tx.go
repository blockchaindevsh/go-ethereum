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

type intHeap []int

func (h intHeap) Len() int           { return len(h) }
func (h intHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h intHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *intHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *intHeap) Pop() interface{} {
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
	groupID := make(map[common.Address]int, 0)

	for index, sender := range from {
		rootAddr := Find(sender)
		id, exist := groupID[rootAddr]
		if !exist {
			id = len(groupList)
			groupID[rootAddr] = id

		}
		groupList[id] = append(groupList[id], index)
	}
	return groupList

}

type txSortManager struct {
	mu   sync.Mutex
	heap *intHeap

	groupLen           int
	groupList          map[int][]int //for fmt
	nextTxIndexInGroup map[int]int
}

func NewSortTxManager(from []common.Address, to []*common.Address) *txSortManager {
	groupList := grouping(from, to)

	common.DebugInfo.Groups += len(groupList)
	maxx := -1
	for _, txs := range groupList {
		l := len(txs)
		if common.DebugInfo.MaxDepeth < l {
			common.DebugInfo.MaxDepeth = l
		}
		if common.DebugInfo.MinDepeth > l {
			common.DebugInfo.MinDepeth = l
		}
		if l > maxx {
			maxx = l
		}
	}
	common.DebugInfo.SumMaxDepth += maxx
	fmt.Println("最大深度", maxx)

	nextTxIndexInGroup := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			nextTxIndexInGroup[list[index]] = list[index+1]
		}
	}

	heapList := make(intHeap, 0)
	for _, v := range groupList {
		heapList = append(heapList, v[0])
	}
	heap.Init(&heapList)

	return &txSortManager{
		heap:               &heapList,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
		groupList:          groupList,
	}
}

func (s *txSortManager) pushNextTxInGroup(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		heap.Push(s.heap, nextTxIndex)
	}
}

func (s *txSortManager) push(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	heap.Push(s.heap, txIndex)
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

	//mu             sync.Mutex
	baseStateDB    *state.StateDB
	mergedReceipts []*types.Receipt
	ch             chan struct{}
	ended          bool

	txSortManger *txSortManager

	txQueue     chan int
	mergedQueue chan struct{}
	resultQueue chan struct{}
	txResults   []*txResult
	gp          uint64
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

		txQueue:     make(chan int, 0),
		mergedQueue: make(chan struct{}, len(block.Transactions())),
		resultQueue: make(chan struct{}, len(block.Transactions())),
		txResults:   make([]*txResult, txLen, txLen),
		gp:          block.GasLimit(),
	}

	signer := types.MakeSigner(bc.chainConfig, block.Number())
	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)
	for _, tx := range block.Transactions() {
		sender, _ := types.Sender(signer, tx)
		fromList = append(fromList, sender)
		toList = append(toList, tx.To())
	}
	go p.baseStateDB.PreCache(fromList, toList)
	p.txSortManger = NewSortTxManager(fromList, toList)

	thread := p.txSortManger.groupLen
	if thread > 16 {
		thread = 16
	}

	for index := 0; index < thread; index++ {
		go p.txLoop()
	}

	go p.schedule()

	go p.mergeLoop()
	fmt.Println("process-", block.NumberU64(), len(block.Transactions()), len(p.txSortManger.groupList))
	return p
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) {
	p.txResults[re.txIndex] = re
	p.resultQueue <- struct{}{}

}

func (p *pallTxManager) txLoop() {
	for !p.ended {
		txIndex, ok := <-p.txQueue
		if !ok {
			break
		}
		p.handleTx(txIndex)
	}
}

func (p *pallTxManager) schedule() {
	for !p.ended {
		if data := p.txSortManger.pop(); data != -1 {
			p.txQueue <- data
		} else {
			_, ok := <-p.mergedQueue
			if !ok {
				break
			}
		}
	}
}

func (p *pallTxManager) mergeLoop() {
	for !p.ended {
		_, ok := <-p.resultQueue
		if !ok {
			break
		}

		startTxIndex := p.baseStateDB.MergedIndex + 1
		handle := false
		for startTxIndex < p.txLen && p.txResults[startTxIndex] != nil {
			handle = true
			p.handleReceipt(p.txResults[startTxIndex])
			startTxIndex = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true
			p.baseStateDB.FinalUpdateObjs(p.block.Coinbase())
			close(p.mergedQueue)
			close(p.txQueue)
			p.ch <- struct{}{}
			return
		}
		if handle && !p.ended {
			p.mergedQueue <- struct{}{}
		}

	}
}

func (p *pallTxManager) handleReceipt(rr *txResult) {
	if rr.receipt != nil && !rr.st.Conflict(p.block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), p.block.Transactions()[rr.txIndex].GasPrice())
		rr.st.Merge(p.baseStateDB, p.block.Coinbase(), txFee)

		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.txIndex] = rr.receipt
		//fmt.Println("<<<<<<<<<<<<<<<<<<<<<", rr.txIndex)
		p.txSortManger.pushNextTxInGroup(rr.txIndex)
		return
	}
	p.txResults[rr.txIndex] = nil
	common.DebugInfo.Conflicts++
	//fmt.Println("??????????", rr.txIndex)
	p.txSortManger.push(rr.txIndex)
}

func (p *pallTxManager) handleTx(txIndex int) {
	tx := p.block.Transactions()[txIndex]
	st, _ := state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)

	st.MergedSts = p.baseStateDB.MergedSts
	st.MergedIndex = p.baseStateDB.MergedIndex
	gas := p.gp

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil && st.MergedIndex+1 == txIndex {
		fmt.Println("---apply tx err---", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex)
	}

	go p.AddReceiptToQueue(&txResult{
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
}

func (p *pallTxManager) GetReceiptsAndLogs() (types.Receipts, []*types.Log, uint64) {
	logs := make([]*types.Log, 0)
	cumulativeGasUsed := uint64(0)
	//fmt.Println("30777", p.mergedReceipts)

	for index := 0; index < p.txLen; index++ {
		//fmt.Println("index", index, p.mergedReceipts[index] == nil)
		cumulativeGasUsed = cumulativeGasUsed + p.mergedReceipts[index].GasUsed
		p.mergedReceipts[index].CumulativeGasUsed = cumulativeGasUsed
		logs = append(logs, p.mergedReceipts[index].Logs...)
	}
	return p.mergedReceipts, logs, cumulativeGasUsed
}
