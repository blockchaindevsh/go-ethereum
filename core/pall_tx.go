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
	fatherMp = make(map[common.Address]common.Address, 0)
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

func CalGroup(from []common.Address, to []*common.Address) map[int][]int {
	fatherMp = make(map[common.Address]common.Address, 0)
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
	mu        sync.Mutex
	heap      *IntHeap
	groupList map[int][]int

	dependMp map[int]int
}

func NewSortTxManager(from []common.Address, to []*common.Address) *txSortManager {
	groupList := CalGroup(from, to)

	dependMp := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			dependMp[list[index]] = list[index+1]
		}
	}

	heapList := make(IntHeap, 0)
	for _, v := range groupList {
		heapList = append(heapList, v[0])
	}
	heap.Init(&heapList)

	return &txSortManager{
		heap:      &heapList,
		groupList: groupList,
		dependMp:  dependMp,
	}
}

func (s *txSortManager) POP() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return -1
	}
	txIndex := heap.Pop(s.heap).(int)
	if s.dependMp[txIndex] != 0 {
		heap.Push(s.heap, s.dependMp[txIndex])
	}
	return txIndex
}

type pallTxManager struct {
	block *types.Block
	txLen int
	bc    *BlockChain

	mubase         sync.RWMutex
	baseStateDB    *state.StateDB
	mergedReceipts []*types.Receipt
	ch             chan struct{}
	ended          bool

	txSortManger *txSortManager

	txQueue      chan int
	receiptQueue []*ReceiptWithIndex
	gp           uint64

	reHandle *handleMap
}

type handleMap struct {
	mp map[int]map[int]bool
	mu sync.Mutex
}

func NewHandleMap(txLen int) *handleMap {
	mp := make(map[int]map[int]bool, 0)
	for base := -1; base <= txLen; base++ {
		mp[base] = make(map[int]bool)

		for i := -1; i <= txLen; i++ {
			mp[base][i] = false
		}
	}
	return &handleMap{
		mp: mp,
		mu: sync.Mutex{},
	}

}

func (h *handleMap) SetValue(base, txindex int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.mp[base][txindex] = true
}

func (h *handleMap) AlreadyHandle(base, txindex int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.mp[base][txindex]
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

		txQueue:      make(chan int, txLen),
		receiptQueue: make([]*ReceiptWithIndex, txLen, txLen),
		gp:           block.GasLimit(),
		reHandle:     NewHandleMap(len(block.Transactions())),
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

	fmt.Println("PALL TX READY", block.Number(), p.txSortManger.groupList)

	thread := len(p.txSortManger.groupList)
	if thread > 8 {
		thread = 8
	}

	p.txQueue = make(chan int, thread)
	for index := 0; index < thread; index++ {
		go p.txLoop()
	}

	for index := 0; index < thread; index++ {
		p.AddTxToQueue(p.txSortManger.POP())
	}
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
	//fmt.Println("MMMMMMMMMM-start", rr.st.MergedIndex, rr.txIndex)
	if rr.st.CheckConflict(p.block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), p.block.Transactions()[rr.txIndex].GasPrice())
		rr.st.Merge(p.baseStateDB, p.block.Coinbase(), txFee)
		fmt.Println("MMMMMMMMMMMMMMMMMMMMM", rr.txIndex, rr.receipt.GasUsed)
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.txIndex] = rr.receipt

		if p.baseStateDB.MergedIndex+1 < p.txLen {
			p.AddTxToQueue(p.txSortManger.POP())
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
	if p.reHandle.AlreadyHandle(st.MergedIndex, txIndex) {
		//fmt.Println("??????",st.MergedIndex,txIndex)
		return false
	}

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	fmt.Println("RRRRRRRRRRRRRReeeeeeee-start", st.MergedIndex, txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	p.reHandle.SetValue(st.MergedIndex, txIndex)
	fmt.Println("RRRRRRRRRRRRRReeeeeeee-end", st.MergedIndex, txIndex, err)
	if err != nil {
		if st.MergedIndex+1 == txIndex {
			fmt.Println("---apply tx err---", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex, "groupList", p.txSortManger.groupList)
			panic("should panic")
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
