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
	//fmt.Println("最大深度", maxx)

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
	blocks types.Blocks

	rewardPoint     []int
	nextRewardPoint int
	coinbaseList    []common.Address
	mpToRealIndex   []*txIndex

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
	index   int
	receipt *types.Receipt
}

type txIndex struct {
	blockIndex int
	tx         int
}

func NewPallTxManage(blockList types.Blocks, st *state.StateDB, bc *BlockChain) *pallTxManager {

	fmt.Println("准备执行", "基于", bc.CurrentBlock().Number(), "from", blockList[0].NumberU64(), blockList[len(blockList)-1].NumberU64())

	errCnt = 0
	txLen := 0
	gp := uint64(0)
	rewardPoint := make([]int, 0)
	coinbaseList := make([]common.Address, 0)
	mpToRealIndex := make([]*txIndex, 0)

	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)
	for blockIndex, block := range blockList {
		signer := types.MakeSigner(bc.chainConfig, block.Number())
		for tIndex, tx := range block.Transactions() {
			sender, _ := types.Sender(signer, tx)
			fromList = append(fromList, sender)
			toList = append(toList, tx.To())
			mpToRealIndex = append(mpToRealIndex, &txIndex{
				blockIndex: blockIndex,
				tx:         tIndex,
			})
		}
		txLen += len(block.Transactions())
		gp += block.GasLimit()
		rewardPoint = append(rewardPoint, txLen)
		coinbaseList = append(coinbaseList, block.Coinbase())

		types.BlockAndHash[block.NumberU64()] = block.Header()
	}

	p := &pallTxManager{
		blocks:        blockList,
		rewardPoint:   rewardPoint,
		coinbaseList:  coinbaseList,
		mpToRealIndex: mpToRealIndex,

		txLen: txLen,
		bc:    bc,

		baseStateDB:    st,
		mergedReceipts: make([]*types.Receipt, txLen, txLen),
		ch:             make(chan struct{}, 1),

		txQueue:     make(chan int, 0),
		mergedQueue: make(chan struct{}, txLen),
		resultQueue: make(chan struct{}, txLen),
		txResults:   make([]*txResult, txLen, txLen),
		gp:          gp,
	}

	p.txSortManger = NewSortTxManager(fromList, toList)

	//fmt.Println("ready to fin init")
	index := 0
	for index < len(p.blocks) {
		block := p.blocks[index]
		if len(block.Transactions()) == 0 {
			p.Fi(index)
		} else {
			break
		}
		index++
	}
	fmt.Println("首次奖励", index, "奖励点", rewardPoint)
	if index == len(p.blocks) {
		p.baseStateDB.FinalUpdateObjs(p.coinbaseList)
		return p
	}
	p.nextRewardPoint = rewardPoint[index]
	thread := p.txSortManger.groupLen
	if thread > 16 {
		thread = 16
	}

	for index := 0; index < thread; index++ {
		go p.txLoop()
	}

	go p.schedule()

	go p.mergeLoop()
	fmt.Println("process-", blockList[0].Number(), blockList[4].Number(), len(p.txSortManger.groupList), p.txSortManger.groupList)
	return p
}
func (p *pallTxManager) Fi(blockIndex int) {
	block := p.blocks[blockIndex]
	p.bc.engine.Finalize(p.bc, block.Header(), p.baseStateDB, block.Transactions(), block.Uncles())
	fmt.Println("开搞", block.NumberU64(), len(block.Transactions()), block.Coinbase().String(), p.baseStateDB.GetBalance(block.Coinbase()), p.rewardPoint[blockIndex]-1)
	p.baseStateDB.MergeReward(p.rewardPoint[blockIndex] - 1)
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) {
	p.txResults[re.index] = re
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
			if succ := p.handleReceipt(p.txResults[startTxIndex]); !succ {
				break
			}
			fmt.Println("合并完毕", "现在", p.baseStateDB.MergedIndex, "总长度", p.txLen, "下次奖励点", p.nextRewardPoint)

			for p.nextRewardPoint == p.baseStateDB.MergedIndex+1 {
				sbBlockIndex := p.mpToRealIndex[p.baseStateDB.MergedIndex].blockIndex

				p.Fi(sbBlockIndex)

				sbBlockIndex++

				//fmt.Println("?????", sbBlockIndex, p.txLen, len(p.blocks[sbBlockIndex].Transactions()))
				for sbBlockIndex < len(p.blocks) && len(p.blocks[sbBlockIndex].Transactions()) == 0 {
					p.Fi(sbBlockIndex)
					sbBlockIndex++
				}
				if sbBlockIndex < len(p.blocks) {
					//fmt.Println("340000", sbBlockIndex, p.rewardPoint[sbBlockIndex])
					p.nextRewardPoint = p.rewardPoint[sbBlockIndex]
				} else {
					break
				}

			}

			startTxIndex = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true
			//fmt.Println("FFFFFFFFFFFFFFFFFFFFFF", p.blocks[0])
			p.baseStateDB.FinalUpdateObjs(p.coinbaseList)
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

func (p *pallTxManager) handleReceipt(rr *txResult) bool {
	block := p.blocks[p.mpToRealIndex[rr.index].blockIndex]
	if rr.receipt != nil && !rr.st.Conflict(block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), block.Transactions()[p.mpToRealIndex[rr.index].tx].GasPrice())
		rr.st.Merge(p.baseStateDB, block.Coinbase(), txFee)

		p.baseStateDB.MergedIndex = rr.index
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.index] = rr.receipt

		p.txSortManger.pushNextTxInGroup(rr.index)
		return true
	}
	p.txResults[rr.index] = nil
	common.DebugInfo.Conflicts++
	//fmt.Println("??????????", rr.txIndex)
	p.txSortManger.push(rr.index)
	return false
}

var (
	errCnt = 0
)

func (p *pallTxManager) handleTx(index int) {
	block := p.blocks[p.mpToRealIndex[index].blockIndex]
	txRealIndex := p.mpToRealIndex[index].tx

	tx := block.Transactions()[txRealIndex]
	st, _ := state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)

	st.MergedSts = p.baseStateDB.MergedSts
	st.MergedIndex = p.baseStateDB.MergedIndex
	gas := p.gp

	st.Prepare(tx.Hash(), block.Hash(), txRealIndex, index)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		fmt.Println("---apply tx err---", err, "blockNumber", block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", index, "realIndex", txRealIndex, "rewardList", p.rewardPoint)
		if errCnt > 1000 {
			panic(err)
		}
		errCnt++
		for _, v := range p.rewardPoint {
			if st.MergedIndex+1 == v {
				//panic(err)
			}
		}

	}

	p.AddReceiptToQueue(&txResult{
		st:      st,
		index:   index,
		receipt: receipt,
	})
}

func (p *pallTxManager) GetReceiptsAndLogs() ([]types.Receipts, [][]*types.Log, []uint64) {
	logList := make([][]*types.Log, 0)
	rsList := make([]types.Receipts, 0)
	usdList := make([]uint64, 0)

	start := 0
	for _, block := range p.blocks {
		if len(block.Transactions()) == 0 {
			logList = append(logList, make([]*types.Log, 0))
			rsList = append(rsList, make(types.Receipts, 0))
			usdList = append(usdList, 0)
			continue
		}
		cumulativeGasUsed := uint64(0)
		log := make([]*types.Log, 0)
		rs := make(types.Receipts, 0)
		ll := len(block.Transactions())

		for i := start; i < start+ll; i++ {
			cumulativeGasUsed = cumulativeGasUsed + p.mergedReceipts[i].GasUsed
			p.mergedReceipts[i].CumulativeGasUsed = cumulativeGasUsed
			log = append(log, p.mergedReceipts[i].Logs...)
			rs = append(rs, p.mergedReceipts[i])
		}
		start += ll
		logList = append(logList, log)
		rsList = append(rsList, rs)
		usdList = append(usdList, cumulativeGasUsed)

	}

	return rsList, logList, usdList
}
