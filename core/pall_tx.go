package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"sync/atomic"
)

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
	//mu        sync.Mutex
	//heapExist map[int]bool

	onPending []bool

	heap               []int
	groupLen           int
	nextTxIndexInGroup map[int]int
	preTxIndexInGroup  map[int]int

	mapIndexToGroupID map[int]int
	pall              *pallTxManager
}

func NewSortTxManager(from []common.Address, to []*common.Address) *txSortManager {
	groupList, indexToID := grouping(from, to)
	fmt.Println("gropuList", groupList)

	nextTxIndexInGroup := make(map[int]int)
	preTxIndexInGroup := make(map[int]int)
	heapList := make([]int, 0)
	for _, list := range groupList {
		for index := 0; index < len(list); index++ {
			if index+1 <= len(list)-1 {
				nextTxIndexInGroup[list[index]] = list[index+1]
			}
			if index-1 >= 0 {
				preTxIndexInGroup[list[index]] = list[index-1]
			}
		}
		heapList = append(heapList, list[0])
	}

	return &txSortManager{
		onPending:          make([]bool, len(from), len(from)),
		heap:               heapList,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
		preTxIndexInGroup:  preTxIndexInGroup,
		mapIndexToGroupID:  indexToID,
	}
}

func (s *txSortManager) pushNextTxInGroup(txIndex int) {
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		fmt.Println("nexxxxxxxxxxxxxxxxx", nextTxIndex)
		s.push(nextTxIndex, true)
		fmt.Println("nexxxxxxxxxxxxxxxxx-end", nextTxIndex)
	}
}

func (s *txSortManager) push(txIndex int, next bool) {
	if s.onPending[txIndex] {
		return
	}
	s.onPending[txIndex] = true

	fmt.Println("push", next, !s.pall.ended, s.pall.txResults[txIndex] == nil, !s.pall.isRunning[txIndex], txIndex)
	if !s.pall.ended && s.pall.txResults[txIndex] == nil && !s.pall.isRunning[txIndex] {
		fmt.Println("txIndex", next, txIndex, len(s.pall.txQueue), s.pall.txLen)
		s.onPending[txIndex] = true
		s.pall.txQueue <- txIndex

		fmt.Println("====end", txIndex)
	} else {
		s.onPending[txIndex] = false
	}
}

type pallTxManager struct {
	randomSeed int32

	//isMerge   []bool
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

	txQueue chan int
	//mergedQueue chan struct{}
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

	mpToRealIndex := make([]*txIndex, 0)

	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)

	minerAndUncle := make([]map[common.Address]bool, 0)
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
	//go st.PreCache(fromList, toList)

	p := &pallTxManager{
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

		txQueue:     make(chan int, txLen),
		resultQueue: make(chan struct{}, txLen),
		txResults:   make([]*txResult, txLen, txLen),
		gp:          gp,
	}

	p.txSortManger = NewSortTxManager(fromList, toList)
	p.txSortManger.pall = p

	for _, v := range p.txSortManger.heap {
		p.txQueue <- v
	}

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
	//go p.schedule()
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
		fmt.Println("can not save", re.index)
		return false
	}
	if p.txResults[re.index] == nil {
		re.seed = p.getSeed()
		p.txResults[re.index] = re
		return true
	} else {
		fmt.Println("already have resulet", re.index)
		return false
	}

}

func (p *pallTxManager) txLoop() {
	for !p.ended {
		txIndex, ok := <-p.txQueue
		if !ok {
			break
		}
		p.txSortManger.onPending[txIndex] = false
		//fmt.Println("txLoop", txIndex, p.isRunning[txIndex], p.txResults[txIndex] != nil)
		if p.isRunning[txIndex] || p.txResults[txIndex] != nil {
			continue
		}
		p.isRunning[txIndex] = true
		stats := p.handleTx(txIndex)
		p.isRunning[txIndex] = false
		fmt.Println("handle tx end", stats, txIndex, p.baseStateDB.MergedIndex)
		if stats {
			p.txSortManger.pushNextTxInGroup(txIndex)
			if p.txLen != len(p.resultQueue) && !p.ended {
				p.resultQueue <- struct{}{}
			}
		} else {
			if txIndex > p.baseStateDB.MergedIndex {
				fmt.Println("push-1", txIndex)
				p.txSortManger.push(txIndex, false)
				fmt.Println("push-2", txIndex)
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
		handle := false

		startTxIndex := p.baseStateDB.MergedIndex + 1
		for startTxIndex < p.txLen && p.txResults[startTxIndex] != nil {
			rr := p.txResults[startTxIndex]
			//fmt.Println("处理收据", "fake", rr.useFake, "index", rr.index, "当前base", p.baseStateDB.MergedIndex, "基于", rr.st.MergedIndex, "区块", p.blocks[p.mpToRealIndex[rr.index].blockIndex].NumberU64(), "real tx", p.mpToRealIndex[rr.index].tx, "seed", rr.seed, "baseSeed", rr.st.RandomSeed)

			handle = true
			if succ := p.handleReceipt(rr); !succ {
				p.markNextFailed(rr.index)
				p.txResults[rr.index] = nil
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
			fmt.Println("MMMMMMMMMMM", startTxIndex)

			p.baseStateDB.MergedIndex = startTxIndex
			startTxIndex = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true
			p.baseStateDB.FinalUpdateObjs()
			close(p.txQueue)
			p.ch <- struct{}{}
			return
		}
		if !p.ended && handle {
			nn := p.baseStateDB.MergedIndex + 1
			if p.txResults[nn] == nil {
				fmt.Println("====================================", nn)
				p.txSortManger.push(nn, false)
				fmt.Println("====================================-end", nn)
			}
		}
		//fmt.Println("mergeLoop---end", p.baseStateDB.MergedIndex)
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
		rr.st.Merge(p.baseStateDB, block.Coinbase(), txFee)
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
			fmt.Println("preResult is nil", index, preIndex)
			return false
		}
		if preIndex > p.baseStateDB.MergedIndex {
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

	st.Prepare(tx.Hash(), block.Hash(), txRealIndex, index)
	if p.txResults[index] != nil || index <= p.baseStateDB.MergedIndex {
		fmt.Println("???????????-1", index, p.txResults[index] != nil, p.baseStateDB.MergedIndex)
		return true
	}

	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, block.Header(), tx, nil, p.bc.vmConfig)
	fmt.Println("开始执行交易", "useFake", useFake, "执行", index, "基于", st.MergedIndex, "当前base", p.baseStateDB.MergedIndex, "blockIndex", p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(), "realIndex", p.mpToRealIndex[index].tx, "baseSeed", st.RandomSeed, err)

	if index <= p.baseStateDB.MergedIndex {
		fmt.Println("???????????-2", index, p.baseStateDB.MergedIndex)
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
