package core

import (
	"container/heap"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"sync"
	"sync/atomic"
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

func grouping(from []common.Address, to []*common.Address) (map[int][]int, map[int]int) {
	rootAddr = make(map[common.Address]common.Address, 0)
	for index, sender := range from {
		Union(sender, to[index])
	}

	groupList := make(map[int][]int, 0)
	mpAddrToID := make(map[common.Address]int, 0)
	mpIndexToID := make(map[int]int, 0)

	for index, sender := range from {
		rootAddr := Find(sender)
		id, exist := mpAddrToID[rootAddr]
		if !exist {
			id = len(groupList)
			mpAddrToID[rootAddr] = id

		}
		groupList[id] = append(groupList[id], index)
		mpIndexToID[index] = id
	}
	return groupList, mpIndexToID

}

type txSortManager struct {
	mu        sync.Mutex
	heapExist map[int]bool
	heap      *intHeap

	groupLen           int
	groupList          map[int][]int //for fmt
	nextTxIndexInGroup map[int]int
	preTxIndexInGroup  map[int]int

	mapIndexToGroupID map[int]int
	pall              *pallTxManager
}

func NewSortTxManager(from []common.Address, to []*common.Address) *txSortManager {
	groupList, indexToID := grouping(from, to)
	//fmt.Println("groupList", groupList)

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

	nextTxIndexInGroup := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			nextTxIndexInGroup[list[index]] = list[index+1]
		}
	}

	preTxIndexInGroup := make(map[int]int)
	for _, list := range groupList {
		for index := 1; index < len(list); index++ {
			preTxIndexInGroup[list[index]] = list[index-1]
		}
	}

	heapExist := make(map[int]bool, 0)
	heapList := make(intHeap, 0)
	for _, v := range groupList {
		heapList = append(heapList, v[0])
		heapExist[v[0]] = true
	}
	heap.Init(&heapList)

	return &txSortManager{
		heap:               &heapList,
		heapExist:          heapExist,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
		preTxIndexInGroup:  preTxIndexInGroup,
		mapIndexToGroupID:  indexToID,
	}
}

func (s *txSortManager) pushNextTxInGroup(txIndex int) {
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		s.push(nextTxIndex)

	}
}
func (s *txSortManager) push(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.pall.ended && !s.heapExist[txIndex] && s.pall.txResults[txIndex] == nil {
		heap.Push(s.heap, txIndex)
		s.heapExist[txIndex] = true

		if s.pall.txLen != len(s.pall.mergedQueue) {
			s.pall.mergedQueue <- struct{}{}
		}
	}

}

func (s *txSortManager) pop() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return -1
	}
	data := heap.Pop(s.heap).(int)
	s.heapExist[data] = false
	return data
}

type pallTxManager struct {
	randomSeed int32

	isMerge   []bool
	isRunning []bool
	canotSave []bool

	blocks         types.Blocks
	minersAndUncle []map[common.Address]bool

	mpToRealIndex []*txIndex

	txLen int
	bc    *BlockChain

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
	seed    int32
	useFake bool
	st      *state.StateDB
	index   int
	receipt *types.Receipt
}

type txIndex struct {
	blockIndex int
	tx         int
}

func (p *pallTxManager) getSeed() int32 {
	atomic.AddInt32(&p.randomSeed, 1)
	data := p.randomSeed
	return data
}

func NewPallTxManage(blockList types.Blocks, st *state.StateDB, bc *BlockChain) *pallTxManager {
	fmt.Println("pall", "from", blockList[0].NumberU64(), "to", blockList[len(blockList)-1].NumberU64())
	errCnt = 0
	txLen := 0
	gp := uint64(0)

	coinbaseList := make([]common.Address, 0)
	mpToRealIndex := make([]*txIndex, 0)

	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)

	minerAndUncle := make([]map[common.Address]bool, 0)
	for blockIndex, block := range blockList {
		common.DebugInfo.Txs += len(block.Transactions())
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
		coinbaseList = append(coinbaseList, block.Coinbase())
		types.BlockAndHash[block.NumberU64()] = block.Header()

		mp := make(map[common.Address]bool)
		mp[block.Coinbase()] = true
		for index := 0; index < blockIndex; index++ {
			mp[blockList[index].Coinbase()] = true
			for _, v := range blockList[index].Uncles() {
				mp[v.Coinbase] = true
			}
		}
		minerAndUncle = append(minerAndUncle, mp)
	}
	go st.PreCache(fromList, toList)

	p := &pallTxManager{
		isMerge:        make([]bool, txLen, txLen),
		isRunning:      make([]bool, txLen, txLen),
		canotSave:      make([]bool, txLen, txLen),
		blocks:         blockList,
		minersAndUncle: minerAndUncle,

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
	p.txSortManger.pall = p

	//for index := 0; index < txLen; index++ {
	//	fmt.Println("index", index, "blockIndex", blockList[p.mpToRealIndex[index].blockIndex].NumberU64(), "realIndexInBlock", p.mpToRealIndex[index].tx)
	//}

	index := 0
	for index < len(p.blocks) {
		block := p.blocks[index]
		if len(block.Transactions()) == 0 {
			p.calBlockReward(index, 0)
		} else {
			break
		}
		index++
	}
	if index == len(p.blocks) {
		p.baseStateDB.FinalUpdateObjs()
		return p
	}

	thread := p.txSortManger.groupLen
	if thread > 32 {
		thread = 32
	}

	for index := 0; index < thread; index++ {
		go p.txLoop()
	}
	go p.schedule()
	go p.mergeLoop()
	return p
}
func (p *pallTxManager) calBlockReward(blockIndex int, txIndex int) {
	block := p.blocks[blockIndex]
	p.bc.engine.Finalize(p.bc, block.Header(), p.baseStateDB, block.Transactions(), block.Uncles())
	if block.NumberU64() == p.bc.Config().DAOForkBlock.Uint64()-1 {
		misc.ApplyDAOHardFork(p.baseStateDB)
	}

	p.baseStateDB.MergeReward(txIndex)
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) bool {
	if p.canotSave[re.index] {
		p.canotSave[re.index] = false
		return false
	}
	if !p.isMerge[re.index] {
		re.seed = p.getSeed()
		p.txResults[re.index] = re
		return true
	} else {
		return false
	}

}

func (p *pallTxManager) txLoop() {
	for !p.ended {
		txIndex, ok := <-p.txQueue
		if !ok {
			break
		}
		stats := p.handleTx(txIndex)
		p.isRunning[txIndex] = false

		if stats {
			p.txSortManger.pushNextTxInGroup(txIndex)
			if p.txLen != len(p.resultQueue) {
				p.resultQueue <- struct{}{}
			}
		} else {
			if txIndex > p.baseStateDB.MergedIndex {
				p.txSortManger.push(txIndex)
			}

		}
	}
}

func (p *pallTxManager) schedule() {
	for !p.ended {
		if data := p.txSortManger.pop(); data != -1 {
			if p.txResults[data] == nil && !p.isRunning[data] {
				p.isRunning[data] = true
				p.txQueue <- data
			}
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
		for startTxIndex < p.txLen && p.txResults[startTxIndex] != nil && !p.isRunning[startTxIndex] {
			rr := p.txResults[startTxIndex]
			//fmt.Println("处理收据", "fake", rr.useFake, "index", rr.index, "当前base", p.baseStateDB.MergedIndex, "基于", rr.st.MergedIndex, "区块", p.blocks[p.mpToRealIndex[rr.index].blockIndex].NumberU64(), "real tx", p.mpToRealIndex[rr.index].tx, "seed", rr.seed, "baseSeed", rr.st.RandomSeed)

			if succ := p.handleReceipt(rr); !succ {
				p.markNextFailed(rr.index)
				p.txResults[rr.index] = nil
				common.DebugInfo.Conflicts++
				p.txSortManger.push(rr.index)
				break
			}

			if p.mpToRealIndex[rr.index].tx == len(p.blocks[p.mpToRealIndex[rr.index].blockIndex].Transactions())-1 {
				startBlockIndex := p.mpToRealIndex[rr.index].blockIndex
				for true {
					p.calBlockReward(startBlockIndex, rr.index)
					startBlockIndex++
					if startBlockIndex < len(p.blocks) && len(p.blocks[startBlockIndex].Transactions()) == 0 {
						continue
					} else {
						break
					}
				}
			}

			p.baseStateDB.MergedIndex = startTxIndex
			//fmt.Println("MMMMMMMMMMMMMM", p.baseStateDB.MergedIndex)
			startTxIndex = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true
			p.baseStateDB.FinalUpdateObjs()
			close(p.mergedQueue)
			close(p.txQueue)
			p.ch <- struct{}{}
			return
		}
		if !p.ended {
			nn := p.baseStateDB.MergedIndex + 1
			if p.txResults[nn] == nil {
				p.txSortManger.push(nn)
			}
		}
	}
}

func (p *pallTxManager) markNextFailed(next int) {
	for true {
		var ok bool
		next, ok = p.txSortManger.nextTxIndexInGroup[next]
		if !ok {
			break
		}
		if p.txResults[next] != nil {
			p.txResults[next] = nil
		} else {
			if p.isRunning[next] {
				p.canotSave[next] = true
			}
			break
		}
	}
}
func (p *pallTxManager) handleReceipt(rr *txResult) bool {
	if rr.useFake && rr.st.RandomSeed != p.txResults[rr.st.MergedIndex].seed {
		//fmt.Println("chongtu---", rr.useFake, rr.st.RandomSeed, p.txResults[rr.st.MergedIndex].seed)
		return false
	}

	block := p.blocks[p.mpToRealIndex[rr.index].blockIndex]
	if rr.receipt != nil && !rr.st.Conflict(p.baseStateDB, p.minersAndUncle[p.mpToRealIndex[rr.index].blockIndex], rr.useFake, p.txSortManger.mapIndexToGroupID) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), block.Transactions()[p.mpToRealIndex[rr.index].tx].GasPrice())
		p.isMerge[rr.index] = true
		rr.st.Merge(p.baseStateDB, block.Coinbase(), txFee)
		p.isMerge[rr.index] = false
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.index] = rr.receipt
		return true
	}
	return false
}

var (
	errCnt = 0
)

func (p *pallTxManager) handleTx(index int) bool {
	block := p.blocks[p.mpToRealIndex[index].blockIndex]
	txRealIndex := p.mpToRealIndex[index].tx
	tx := block.Transactions()[txRealIndex]

	var st *state.StateDB
	useFake := false
	preIndex, existPre := p.txSortManger.preTxIndexInGroup[index]
	if !existPre {
		st, _ = state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
		st.MergedIndex = p.baseStateDB.MergedIndex
	} else {
		preResult := p.txResults[preIndex]
		if preResult == nil {
			return false
		}
		if preIndex > p.baseStateDB.MergedIndex {
			if p.isMerge[preIndex] {
				return false
			}
			//fmt.Println("ready copy", index)
			st = preResult.st.Copy()
			//fmt.Println("end copy", index)
			st.MergedIndex = preIndex
			st.RandomSeed = preResult.seed
			useFake = true
		} else {
			st, _ = state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
			st.MergedIndex = p.baseStateDB.MergedIndex
		}
	}
	st.MergedSts = p.baseStateDB.MergedSts
	gas := p.gp

	//fmt.Println("开始执行交易", "useFake", useFake, "执行", index, "基于", st.MergedIndex, "当前base", p.baseStateDB.MergedIndex, "blockIndex", p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(), "realIndex", p.mpToRealIndex[index].tx, "baseSeed", st.RandomSeed)
	st.Prepare(tx.Hash(), block.Hash(), txRealIndex, index)
	if p.txResults[index] != nil || index <= p.baseStateDB.MergedIndex {
		return true
	}
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, block.Header(), tx, nil, p.bc.vmConfig)
	if index <= p.baseStateDB.MergedIndex {
		return true
	}

	if err != nil && st.MergedIndex+1 == index && st.MergedIndex == p.baseStateDB.MergedIndex && !useFake {
		errCnt++
		if errCnt > 100 {
			fmt.Println("?????????", st.MergedIndex, index, p.baseStateDB.MergedIndex, useFake)
			fmt.Println("sbbbbbbbbbbbb", "useFake", useFake, "执行", index, "基于", st.MergedIndex, "当前base", p.baseStateDB.MergedIndex, "blockIndex", p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(), "realIndex", p.mpToRealIndex[index].tx, "baseSeed", st.RandomSeed)
			panic(err)
		}
	}
	p.markNextFailed(index)
	return p.AddReceiptToQueue(&txResult{
		useFake: useFake,
		st:      st,
		index:   index,
		receipt: receipt,
	})
}

func (p *pallTxManager) GetReceiptsAndLogs() ([]types.Receipts, [][]*types.Log, []uint64) {
	logList := make([][]*types.Log, 0)
	rsList := make([]types.Receipts, 0)
	usdList := make([]uint64, 0)
	ans := ""
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
			for _, sb := range p.mergedReceipts[i].Logs {
				//fmt.Println("i", i, sb.Address.String(), sb.TxIndex, sb.Index, len(sb.Topics))
				ans += fmt.Sprintf("+%v", i)
				ans += sb.Address.String()
				for _, vv := range sb.Topics {
					//fmt.Println("tt", vv.String())
					ans += "-"
					ans += vv.String()
				}
			}
			rs = append(rs, p.mergedReceipts[i])
		}
		start += ll

		logList = append(logList, log)
		rsList = append(rsList, rs)
		usdList = append(usdList, cumulativeGasUsed)

	}

	//fmt.Println("ans", ans)
	return rsList, logList, usdList
}
