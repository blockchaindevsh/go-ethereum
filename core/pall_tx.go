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
	fatherMp = make(map[common.Address]common.Address, 0) //https://etherscan.io/txs?block=342007
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
		//txIndexToGroup[index] = groupID
	}
	return groupList

}

type SortTxManager struct {
	heap      *IntHeap
	groupList map[int][]int

	dependMp map[int]int
}

func NewSortTxManager(from []common.Address, to []*common.Address) *SortTxManager {
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

	return &SortTxManager{
		heap:      &heapList,
		groupList: groupList,
		dependMp:  dependMp,
	}
}

//func (s *SortTxManager) sameGroup(a, b int) bool {
//	return s.txIndexToGroup[a] == s.txIndexToGroup[b]
//}
//
//func (s *SortTxManager) AddTx(txIndex int) {
//	for index := 0; index < len(*s.heap); index++ {
//		if s.sameGroup((*s.heap)[index], txIndex) {
//			(*s.heap)[index] = txIndex
//			heap.Fix(s.heap, index)
//			return
//		}
//	}
//	heap.Push(s.heap, txIndex)
//}

func (s *SortTxManager) POP() int {
	if s.heap.Len() == 0 {
		//debug.PrintStack()
		//fmt.Println("1313131321")
		return -1
	}
	txIndex := heap.Pop(s.heap).(int)
	if s.dependMp[txIndex] != 0 {
		heap.Push(s.heap, s.dependMp[txIndex])
	}
	//fmt.Println("TTTTTTTTTTTTT", txIndex)
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

	sortManger *SortTxManager

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
		ch:    make(chan struct{}, 1),

		baseStateDB:    st,
		mergedReceipts: make([]*types.Receipt, txLen, txLen),

		txQueue:      make(chan int, txLen),
		receiptQueue: make([]*ReceiptWithIndex, txLen, txLen),
		gp:           block.GasLimit(),
	}

	signer := types.MakeSigner(bc.chainConfig, block.Number())

	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)
	for _, tx := range block.Transactions() {
		sender, _ := types.Sender(signer, tx)
		fromList = append(fromList, sender)
		toList = append(toList, tx.To())
	}
	p.sortManger = NewSortTxManager(fromList, toList)

	//if common.PrintExtraLog {
	fmt.Println("PALL TX READY", block.Number(), p.sortManger.groupList)
	//}

	thread := len(p.sortManger.groupList)
	if thread > 8 {
		thread = 8
	}
	p.txQueue = make(chan int, thread)
	for index := 0; index < thread; index++ {
		go p.txLoop()
	}

	for index := 0; index < thread; index++ {
		if data := p.sortManger.POP(); data != -1 {
			p.AddTxToQueue(data)
		}
		//fmt.Println("tttttttttt---", index, thread, t)

	}
	return p
}

func (p *pallTxManager) AddTxToQueue(txIndex int) {
	if txIndex == -1 {
		//fmt.Println("SB_____>>>>")
		//debug.PrintStack()
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
	if rr.st.CheckConflict(p.block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), p.block.Transactions()[rr.txIndex].GasPrice())
		rr.st.Merge(p.baseStateDB, p.block.Coinbase(), txFee)
		p.baseStateDB.MergedIndex = rr.txIndex
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.txIndex] = rr.receipt

		if p.baseStateDB.MergedIndex+1 < p.txLen {
			if txIndex := p.sortManger.POP(); txIndex != -1 {
				//fmt.Println("GGGGGGGGGGGGGGGGGGGGG", rr.txIndex, txIndex)
				p.AddTxToQueue(txIndex)
			}
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
	st.Sts = p.baseStateDB.Sts
	st.MergedIndex = p.baseStateDB.MergedIndex
	gas := p.gp
	p.mubase.Unlock()

	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	//fmt.Println("RRRRRRRRRRRRRReeeeeeee-start", st.MergedIndex, txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, p.block.Header(), tx, nil, p.bc.vmConfig)
	//fmt.Println("RRRRRRRRRRRRRReeeeeeee", st.MergedIndex, txIndex, err)
	if err != nil {
		if st.MergedIndex+1 == txIndex {
			fmt.Println("---apply tx err---", err, "blockNumber", p.block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", txIndex, "groupList", p.sortManger.groupList)

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
