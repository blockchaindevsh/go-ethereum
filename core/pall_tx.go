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

	pall *pallTxManager
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
		//fmt.Println("pushNextTxInGroup",nextTxIndex,s.heapExist[nextTxIndex],s.pall.IsRunning())
		if !s.heapExist[nextTxIndex]&& !s.pall.IsRunning(nextTxIndex){
			fmt.Println("pushNextTxInGroup---",nextTxIndex)
			heap.Push(s.heap, nextTxIndex)
			s.heapExist[nextTxIndex]=true

			if s.pall.txLen!=len(s.pall.mergedQueue){
				s.pall.mergedQueue<- struct{}{}
			}
		}

	}
}

func (s *txSortManager) push(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//fmt.Println("push",txIndex)
	if !s.heapExist[txIndex] && !s.pall.IsRunning(txIndex){
		fmt.Println("push---",txIndex)
		heap.Push(s.heap, txIndex)
		s.heapExist[txIndex]=true

		if s.pall.txLen!=len(s.pall.mergedQueue){

			s.pall.mergedQueue<- struct{}{}
			fmt.Println("mergeQueue<-struct{}")
		}
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
	if s.pall.IsRunning(data){
		fmt.Println("Pop isRunning",data)
		return -1
	}
	return data
}

type pallTxManager struct {
	mu sync.Mutex
	exeMp map[int]bool
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
	needFailed []bool
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

	exeMap:=make(map[int]bool,0)
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
			//exeMap[len(mpToRealIndex)-1]=make(map[int]struct{},0)
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
		needFailed:make([]bool,txLen,txLen),
		gp:          gp,
	}

	p.txSortManger = NewSortTxManager(fromList, toList)
	p.txSortManger.pall=p

	fmt.Println("gropuList",p.txSortManger.groupList)
	for index:=0;index<txLen;index++{
		//fmt.Println("txIndex",index,"blockIndex",p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(),"txIndex",p.mpToRealIndex[index].tx)
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
	fmt.Println("FF----")
	//if len(block.Transactions())==0{
		p.bc.engine.Finalize(p.bc, block.Header(), p.baseStateDB, block.Transactions(), block.Uncles())
		if block.NumberU64() == p.bc.Config().DAOForkBlock.Uint64()-1 {
			misc.ApplyDAOHardFork(p.baseStateDB)
		}
	//}

	p.baseStateDB.MergeReward(p.rewardPoint[blockIndex] - 1)
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) {

	p.txResults[re.index] = re
	p.Finally(re.index)
	fmt.Println("Addrecript",re.index,len(p.resultQueue),p.txLen,re.receipt==nil)
	p.resultQueue <- struct{}{}

}

func (p *pallTxManager) txLoop() {
	for !p.ended {
		txIndex, ok := <-p.txQueue
		if !ok {
			break
		}
		fmt.Println("txLoop",txIndex,p.IsRunning(txIndex))
		if p.IsRunning(txIndex){
			fmt.Println("348-------",txIndex)
			continue
		}
		fmt.Println("begin-11",txIndex)
		p.Run(txIndex)
		fmt.Println("begin-22",txIndex)
		p.handleTx(txIndex)
		fmt.Println("begin-33",txIndex)
		p.Finally(txIndex)
		fmt.Println("begin-44",txIndex)
	}
}

func (p *pallTxManager) schedule() {
	for !p.ended {
		if data := p.txSortManger.pop(); data != -1 {
			fmt.Println("schedule",data,p.txResults[data]==nil,!p.IsRunning(data))
			if p.txResults[data]==nil &&!p.IsRunning(data) &&!p.ended{
				p.txQueue <- data
			}

		} else {
			_, ok := <-p.mergedQueue
			fmt.Println("have sched")
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
		//fmt.Println("392-----",startTxIndex,p.txLen,p.txResults[startTxIndex]!=nil)
		for startTxIndex < p.txLen && p.txResults[startTxIndex] != nil {

			rr:=p.txResults[startTxIndex]
			fmt.Println("处理收据",rr.index,"当前base",p.baseStateDB.MergedIndex,"基于",rr.st.MergedIndex,"区块",p.blocks[p.mpToRealIndex[rr.index].blockIndex].NumberU64(),"real tx",p.mpToRealIndex[rr.index].tx)

			if succ := p.handleReceipt(rr); !succ {
				break
			}

			//for p.nextRewardPoint == startTxIndex+1 {
			//	sbBlockIndex := p.mpToRealIndex[startTxIndex].blockIndex
			//
			//	p.Fi(sbBlockIndex)
			//
			//	sbBlockIndex++
			//
			//	for sbBlockIndex < len(p.blocks) && len(p.blocks[sbBlockIndex].Transactions()) == 0 {
			//		p.Fi(sbBlockIndex)
			//		sbBlockIndex++
			//	}
			//	if sbBlockIndex < len(p.blocks) {
			//		p.nextRewardPoint = p.rewardPoint[sbBlockIndex]
			//	} else {
			//		break
			//	}
			//
			//}

			p.baseStateDB.MergedIndex = startTxIndex
			fmt.Println("MMMMMMMMMMMMMM",p.baseStateDB.MergedIndex)
			startTxIndex = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true

			p.Fi(0)
			p.baseStateDB.FinalUpdateObjs(p.coinbaseList)
			close(p.mergedQueue)
			close(p.txQueue)
			p.ch <- struct{}{}
			return
		}
		if  !p.ended {
			nn:=p.baseStateDB.MergedIndex+1
			 if p.txResults[nn]==nil{
			 	fmt.Println("need nn",len(p.mergedQueue),p.txLen)
				p.txSortManger.push(nn)
				//if len(p.mergedQueue)!=p.txLen{
				//	p.mergedQueue <- struct{}{}
				//}
			}else{
				fmt.Println("not need",nn)
			 }
			//fmt.Println("111",p.baseStateDB.MergedIndex,len(p.mergedQueue))

			//fmt.Println("222",p.baseStateDB.MergedIndex,len(p.mergedQueue))
		}
		fmt.Println("结束",p.baseStateDB.MergedIndex)
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



	block := p.blocks[p.mpToRealIndex[rr.index].blockIndex]

	blockIndex:=p.mpToRealIndex[rr.index].blockIndex

	uncles :=make(map[common.Address]bool,0)
	//rewards[block.Coinbase()]=true


	miners:=make(map[common.Address]bool,0)
	for true{
		bb:=p.blocks[blockIndex]
		for _,v:=range bb.Uncles(){
			uncles[v.Coinbase]=true
		}
		miners[bb.Coinbase()]=true
		blockIndex++
		if blockIndex<len(p.blocks)&&len(p.blocks[blockIndex].Transactions())==0{
			continue
		}else{
			break
		}
	}


	lastIndex:=false
	if p.mpToRealIndex[rr.index].tx==len(block.Transactions())-1{
		lastIndex=true
	}

	if rr.receipt != nil && !rr.st.Conflict(miners, uncles,rr.useFake,lastIndex) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), block.Transactions()[p.mpToRealIndex[rr.index].tx].GasPrice())
		rr.st.Merge(p.baseStateDB, block.Coinbase(), txFee)
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.index] = rr.receipt
		//p.txSortManger.pushNextTxInGroup(rr.index)
		return true
	}


	fmt.Println("处理收据失败","当前base",p.baseStateDB.MergedIndex,"待处理的",rr.index,"基于",rr.st.MergedIndex,"区块",p.blocks[p.mpToRealIndex[rr.index].blockIndex].NumberU64(),"real tx",p.mpToRealIndex[rr.index].tx,"receipt?",rr.receipt != nil)
	p.txResults[rr.index] = nil
	next:=rr.index
	for true{
		var ok bool
		next,ok=p.txSortManger.nextTxIndexInGroup[next]
		if !ok {
			//fmt.Println("bbbbbbbbbbbbb")
			break
		}
		if p.txResults[next]!=nil{
			p.txResults[next]=nil
		}else{
			if p.IsRunning(next){
				p.SetFailed(next)
			}else{
				break
			}
		}
	}

	common.DebugInfo.Conflicts++
	p.txSortManger.push(rr.index)
	//if len(p.mergedQueue)!=p.txLen{
	//	p.mergedQueue<- struct{}{}
	//}
	return false
}

var (
	errCnt = 0
)

func (p *pallTxManager)Run(index  int)  {
	p.mu.Lock()
	defer p.mu.Unlock()
	//_,ok:=p.exeMp[index]
	//fmt.Println("index",index,base,ok)
	p.exeMp[index]= true
}

func (p *pallTxManager)Finally(index int)  {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exeMp[index]=false
}

func (p *pallTxManager)IsRunning(index int)  bool{
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exeMp[index]
}

func (p *pallTxManager)SetFailed(rr int)  {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.needFailed[rr]=true
}

func (p *pallTxManager)Skip(rr int)bool  {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.needFailed[rr]{
		p.needFailed[rr]=false
		return true
	}
	return false
}

func (p *pallTxManager) handleTx(index int) {
	sb:=bool(false)
	defer func() {
		if sb{
			p.Finally(index)
			//fmt.Println("564------")
			p.txSortManger.push(index)
			//if len(p.mergedQueue)!=p.txLen && !p.ended{
			//	p.mergedQueue<- struct{}{}
			//}
			fmt.Println("sssssssssssssssssbbb",len(p.mergedQueue),p.txLen)
		}
	}()

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
		sbR :=p.txResults[preIndex]
		if preIndex>p.baseStateDB.MergedIndex && sbR !=nil && sbR.receipt!=nil {
			//fmt.Println("CCCCCCCCCCCCCCCCCCCCCCCCc",index,preIndex)
			if preIndex==p.baseStateDB.MergedIndex+1{
				sb =true
				return
			}
			st= sbR.st.Copy()
			st.MergedIndex=preIndex
			st.Pre= sbR.st.MergedIndex
			useFake=true
		}else{
			st, _ = state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
			st.MergedIndex = p.baseStateDB.MergedIndex
		}
	}


	//if p.has(index,st.MergedIndex)&&p.txResults[index]!=nil{
	//	fmt.Println("545------")
	//	return
	//}
	if index<=st.MergedIndex{
		fmt.Println(">...5409999")
		return
	}


	st.MergedSts = p.baseStateDB.MergedSts
	gas := p.gp

	if p.txResults[index]!=nil{
		return
	}
	fmt.Println("执行交易","useFake",useFake,"执行",index,"当前base",p.baseStateDB.MergedIndex,"基于",st.MergedIndex,"blockIndex",p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(),"realIndex",p.mpToRealIndex[index].tx,"st.stats",st.Print())
	st.Prepare(tx.Hash(), block.Hash(), txRealIndex, index)
	if p.txResults[index]!=nil{
		fmt.Println("nnnnnnnnil")
		return
	}
	//p.Run(index,st.MergedIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, block.Header(), tx, nil, p.bc.vmConfig)

	//if receipt!=nil{
		//fmt.Println("gasUsed",index,receipt.GasUsed)
	//}
	if index<=p.baseStateDB.MergedIndex{
		fmt.Println("meyong")
		return
	}

	if p.Skip(index) || (st.MergedIndex!=-1 &&p.txResults[st.MergedIndex]==nil){
		//fmt.Println("564------")
		p.txSortManger.push(index)
		fmt.Println("????????????????",index,p.Skip(index),p.txResults[st.MergedIndex]==nil)
		if !p.ended{
		sb=true
		}
		return
	}

	//fmt.Println("执行完毕--","执行",index,"当前base",p.baseStateDB.MergedIndex,"基于",st.MergedIndex,"blockIndex",p.blocks[p.mpToRealIndex[index].blockIndex].NumberU64(),"realIndex",p.mpToRealIndex[index].tx,err,"st.stats",st.Print())
	if err != nil {
		//fmt.Println("---apply tx err---", err, "blockNumber", block.NumberU64(), "baseMergedNumber", st.MergedIndex, "currTxIndex", index, "realIndex", txRealIndex)
		if errCnt > 10 {
			panic(err)
		}

		if st.MergedIndex+1==index && st.MergedIndex==p.baseStateDB.MergedIndex&&!useFake{
			errCnt++
			fmt.Println("?????????",st.MergedIndex,index,p.baseStateDB.MergedIndex,useFake)
			//panic(err)
		}
	}
	if err==nil{
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), block.Transactions()[p.mpToRealIndex[index].tx].GasPrice())
		preState:=st.RWSet[block.Coinbase()]
		st.AddBalance(block.Coinbase(),txFee)
		st.RWSet[block.Coinbase()]=preState
		//fmt.Println("xxxxxx",block.Coinbase().String(),preState)
		//if txRealIndex==len(block.Transactions())-1{
		//	p.bc.engine.Finalize(p.bc, block.Header(), st, block.Transactions(), block.Uncles())
		//	nextBlockIndex:=p.mpToRealIndex[index].blockIndex+1
		//	for true{
		//		if nextBlockIndex<len(p.blocks)&&len(p.blocks[nextBlockIndex].Transactions())==0{
		//			block=p.blocks[nextBlockIndex]
		//			p.bc.engine.Finalize(p.bc, block.Header(), st, block.Transactions(), block.Uncles())
		//			nextBlockIndex++
		//		}else{
		//			break
		//		}
		//	}
		//
		//}
	}
	next:=index

	for true{
		var ok bool
		next,ok=p.txSortManger.nextTxIndexInGroup[next]
		if !ok {
			//fmt.Println("bbbbbbbbbbbbb")
			break
		}
		if p.txResults[next]!=nil{
			 p.txResults[next]=nil
		}else{
			if p.IsRunning(next){
				p.SetFailed(next)
			}else{
				break
			}
		}
	}

	//fmt.Println("ready to push next to ",index,p.txSortManger.nextTxIndexInGroup[index])
	p.txSortManger.pushNextTxInGroup(index)
	//if len(p.mergedQueue)!=p.txLen{
	//	p.mergedQueue<- struct{}{}
	//}
	go p.AddReceiptToQueue(&txResult{
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
