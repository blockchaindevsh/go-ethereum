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
	heapExist map[int]bool
	heap *intHeap

	groupLen           int
	groupList          map[int][]int //for fmt
	nextTxIndexInGroup map[int]int
	preTxIndexInGroup map[int]int
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

	nextTxIndexInGroup := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			nextTxIndexInGroup[list[index]] = list[index+1]
		}
	}

	preTxIndexInGroup:=make(map[int]int)
	for _,list:=range groupList{
		for index:=1;index<len(list);index++{
			preTxIndexInGroup[list[index]]=list[index-1]
		}
	}


	heapExist:=make(map[int]bool,0)
	heapList := make(intHeap, 0)
	for _, v := range groupList {
		heapList = append(heapList, v[0])
		heapExist[v[0]]=true
	}
	heap.Init(&heapList)

	return &txSortManager{
		heap:               &heapList,
		heapExist:heapExist,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
		preTxIndexInGroup:preTxIndexInGroup,
		groupList:          groupList,
	}
}

func (s *txSortManager) pushNextTxInGroup(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		//fmt.Println("pushNextTxInGroup",nextTxIndex)
		if !s.heapExist[nextTxIndex]{
			heap.Push(s.heap, nextTxIndex)
			s.heapExist[nextTxIndex]=true
		}

	}
}

func (s *txSortManager) push(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//fmt.Println("push",txIndex)
	if !s.heapExist[txIndex]{
		heap.Push(s.heap, txIndex)
		s.heapExist[txIndex]=true
	}

}

func (s *txSortManager) pop() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return -1
	}
	data:= heap.Pop(s.heap).(int)
	s.heapExist[data]=false
	return data
}

type pallTxManager struct {
	mu sync.Mutex
	exeMp map[int]map[int]struct{}
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
	useFake bool
	st      *state.StateDB
	index   int
	receipt *types.Receipt
}

type txIndex struct {
	blockIndex int
	tx         int
}

func NewPallTxManage(blockList types.Blocks, st *state.StateDB, bc *BlockChain) *pallTxManager {
	fmt.Println("----------------------------- read pall tx ---------------------------------", "base", bc.CurrentBlock().Number(), "from", blockList[0].NumberU64(), "to", blockList[len(blockList)-1].NumberU64())

	if blockList[0].NumberU64()!=bc.CurrentBlock().NumberU64()+1{
		panic(fmt.Errorf("bug here %v %v",blockList[0].NumberU64(),bc.CurrentBlock().NumberU64()))
	}

	errCnt = 0
	txLen := 0
	gp := uint64(0)
	rewardPoint := make([]int, 0)
	coinbaseList := make([]common.Address, 0)
	mpToRealIndex := make([]*txIndex, 0)

	exeMap:=make(map[int]map[int]struct{},0)
	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)
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
			exeMap[len(mpToRealIndex)-1]=make(map[int]struct{},0)
		}
		txLen += len(block.Transactions())
		gp += block.GasLimit()
		rewardPoint = append(rewardPoint, txLen)
		coinbaseList = append(coinbaseList, block.Coinbase())


		types.BlockAndHash[block.NumberU64()] = block.Header()

	}


	p := &pallTxManager{
		exeMp:exeMap,
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

	fmt.Println("gropuList",p.txSortManger.groupList)
	for index:=0;index<txLen;index++{
		fmt.Println("txIndex",index,"blockIndex",p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(),"txIndex",p.mpToRealIndex[index].tx)
	}
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
	return p
}
func (p *pallTxManager) Fi(blockIndex int) {
	block := p.blocks[blockIndex]
	if len(block.Transactions())==0{
		p.bc.engine.Finalize(p.bc, block.Header(), p.baseStateDB, block.Transactions(), block.Uncles())
		if block.NumberU64() == p.bc.Config().DAOForkBlock.Uint64()-1 {
			misc.ApplyDAOHardFork(p.baseStateDB)
		}
	}

	p.baseStateDB.MergeReward(p.rewardPoint[blockIndex] - 1)
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) {

	p.txResults[re.index] = re
	//fmt.Println("Addrecript",re.index,time.Now().String(),p.txResults[re.index]==nil)
	p.resultQueue <- struct{}{}

}

func (p *pallTxManager) txLoop() {
	for !p.ended {
		txIndex, ok := <-p.txQueue
		if !ok {
			break
		}
		//fmt.Println("zhixing",txIndex)
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

			for p.nextRewardPoint == startTxIndex+1 {
				sbBlockIndex := p.mpToRealIndex[startTxIndex].blockIndex

				p.Fi(sbBlockIndex)

				sbBlockIndex++

				for sbBlockIndex < len(p.blocks) && len(p.blocks[sbBlockIndex].Transactions()) == 0 {
					p.Fi(sbBlockIndex)
					sbBlockIndex++
				}
				if sbBlockIndex < len(p.blocks) {
					p.nextRewardPoint = p.rewardPoint[sbBlockIndex]
				} else {
					break
				}

			}

			p.baseStateDB.MergedIndex = startTxIndex
			fmt.Println("MMMMMMMMMMMMMM",p.baseStateDB.MergedIndex)
			startTxIndex = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true
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
	if rr.useFake{
		fmt.Println("rr.useFake,",rr.st.Pre,rr.st.MergedIndex,p.txResults[rr.st.MergedIndex].st.MergedIndex)
		if rr.st.Pre!=p.txResults[rr.st.MergedIndex].st.MergedIndex{
			p.txResults[rr.index] = nil
			common.DebugInfo.Conflicts++
			fmt.Println("=======================",rr.index,rr.st.MergedIndex,rr.st.Pre,p.txResults[rr.st.MergedIndex].st.MergedIndex)
			p.txSortManger.push(rr.index)
			return false
		}
	}


	fmt.Println("处理收据",rr.index,"当前base",p.baseStateDB.MergedIndex,"基于",rr.st.MergedIndex,"区块",p.blocks[p.mpToRealIndex[rr.index].blockIndex].NumberU64(),"real tx",p.mpToRealIndex[rr.index].tx)
	block := p.blocks[p.mpToRealIndex[rr.index].blockIndex]
	if rr.receipt != nil && !rr.st.Conflict(block.Coinbase()) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), block.Transactions()[p.mpToRealIndex[rr.index].tx].GasPrice())
		rr.st.Merge(p.baseStateDB, block.Coinbase(), txFee)
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.index] = rr.receipt

		p.txSortManger.pushNextTxInGroup(rr.index)
		return true
	}

	fmt.Println("处理收据-failed","当前base",p.baseStateDB.MergedIndex,"待处理的",rr.index,"基于",rr.st.MergedIndex,"区块",p.blocks[p.mpToRealIndex[rr.index].blockIndex].NumberU64(),"real tx",p.mpToRealIndex[rr.index].tx)
	p.txResults[rr.index] = nil
	common.DebugInfo.Conflicts++
	fmt.Print("")
	p.txSortManger.push(rr.index)
	return false
}

var (
	errCnt = 0
)

func (p *pallTxManager)has(index ,base int)bool  {
	p.mu.Lock()
	defer p.mu.Unlock()
	_,ok:=p.exeMp[index][base]
	return ok
}

func (p *pallTxManager)Run(index ,base int)  {
	p.mu.Lock()
	defer p.mu.Unlock()
	//_,ok:=p.exeMp[index]
	//fmt.Println("index",index,base,ok)
	p.exeMp[index][base]= struct{}{}
}

func (p *pallTxManager) handleTx(index int) {

	fmt.Println("准备执行交易",index)
	block := p.blocks[p.mpToRealIndex[index].blockIndex]
	txRealIndex := p.mpToRealIndex[index].tx

	tx := block.Transactions()[txRealIndex]

	var st *state.StateDB

	useFake:=false

	preIndex,existPre:=p.txSortManger.preTxIndexInGroup[index]
	if !existPre{
		st, _ = state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
		st.MergedIndex = p.baseStateDB.MergedIndex
	}else{
		if p.txResults[preIndex]!=nil && p.txResults[preIndex].receipt!=nil && preIndex>p.baseStateDB.MergedIndex {
			fmt.Println("CCCCCCCCCCCCCCCCCCCCCCCCc",index,preIndex)
			st=p.txResults[preIndex].st.Copy()
			st.MergedIndex=preIndex
			st.Pre=p.txResults[preIndex].st.MergedIndex
			useFake=true
		}else{
			st, _ = state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
			st.MergedIndex = p.baseStateDB.MergedIndex
		}
	}


	if p.has(index,st.MergedIndex)&&p.txResults[index]!=nil{
		//fmt.Println("??????????",index,st.MergedIndex)
		return
	}
	if index<=st.MergedIndex{
		return
	}
	p.Run(index,st.MergedIndex)


	st.MergedSts = p.baseStateDB.MergedSts
	gas := p.gp

	if p.txResults[index]!=nil{
		return
	}
	fmt.Println("执行交易--",useFake,"执行",index,"当前base",p.baseStateDB.MergedIndex,"基于",st.MergedIndex,"blockIndex",p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(),"realIndex",p.mpToRealIndex[index].tx,"st.stats",st.Print())
	st.Prepare(tx.Hash(), block.Hash(), txRealIndex, index)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, block.Header(), tx, nil, p.bc.vmConfig)
	fmt.Println("执行完毕--","执行",index,"当前base",p.baseStateDB.MergedIndex,"基于",st.MergedIndex,"blockIndex",p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(),"realIndex",p.mpToRealIndex[index].tx,err,"st.stats",st.Print())
	if err != nil {
		fmt.Println("---apply tx err---", err, "blockNumber", block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", index, "realIndex", txRealIndex)
		if errCnt > 100 {
			panic(err)
		}
		errCnt++
		if st.MergedIndex+1==index && st.MergedIndex==p.baseStateDB.MergedIndex&&!useFake{
			fmt.Println("?????????",st.MergedIndex,index,p.baseStateDB.MergedIndex,useFake)
			panic(err)
		}
		for _, v := range p.rewardPoint {
			if st.MergedIndex+1 == v {
				//panic(err)
			}
		}

	}
	if err==nil{
		st.RWSet[block.Coinbase()]=true
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), block.Transactions()[p.mpToRealIndex[index].tx].GasPrice())
		st.AddBalance(block.Coinbase(),txFee)
		if txRealIndex==len(block.Transactions())-1{
			p.bc.engine.Finalize(p.bc, block.Header(), st, block.Transactions(), block.Uncles())
			nextBlockIndex:=p.mpToRealIndex[index].blockIndex+1
			for true{
				if nextBlockIndex<len(p.blocks)&&len(p.blocks[nextBlockIndex].Transactions())==0{
					block=p.blocks[nextBlockIndex]
					p.bc.engine.Finalize(p.bc, block.Header(), st, block.Transactions(), block.Uncles())
					nextBlockIndex++
				}else{
					break
				}
			}

		}
	}
	p.txSortManger.pushNextTxInGroup(index)
	p.AddReceiptToQueue(&txResult{
		useFake:useFake,
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
