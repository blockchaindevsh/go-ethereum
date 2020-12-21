package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/sync/errgroup"
	"math/big"
	"sync/atomic"
	"time"
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
	addrToID := make(map[common.Address]int, 0)
	indexToID := make(map[int]int, 0)

	for index, sender := range from {
		rootAddr := Find(sender)
		id, exist := addrToID[rootAddr]
		if !exist {
			id = len(groupList)
			addrToID[rootAddr] = id

		}
		groupList[id] = append(groupList[id], index)
		indexToID[index] = id
	}
	return groupList, indexToID

}

type groupInfo struct {
	nextTxInGroup  map[int]int
	preTxInGroup   map[int]int
	indexToGroupID map[int]int
}

func newGroupInfo(from []common.Address, to []*common.Address) (*groupInfo, []int, int) {
	groupList, indexToID := grouping(from, to)
	//fmt.Println("gropuList", groupList)

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

	return &groupInfo{
		nextTxInGroup:  nextTxIndexInGroup,
		preTxInGroup:   preTxIndexInGroup,
		indexToGroupID: indexToID,
	}, heapList, len(groupList)
}

func (s *pallTxManager) push(txIndex int) {
	if !atomic.CompareAndSwapInt32(&s.pending[txIndex], 0, 1) {
		return
	}

	fmt.Println("push", !s.ended, s.txResults[txIndex] == nil, txIndex)
	if !s.ended && s.txResults[txIndex] == nil {
		//fmt.Println("txIndex--", txIndex, len(s.txQueue), s.txLen)
		s.txQueue <- txIndex
		//fmt.Println("txIndexend", txIndex)
	} else {
		s.setPending(txIndex, false)
	}
}

type pallTxManager struct {
	resultID int32

	pending    []int32
	needFailed []bool

	blocks         types.Blocks
	minersAndUncle []map[common.Address]bool

	indexInfos []*indexInfo

	txLen int
	bc    *BlockChain

	baseStateDB    *state.StateDB
	mergedReceipts []*types.Receipt
	ch             chan struct{}
	ended          bool

	groupInfo *groupInfo

	txQueue chan int
	//mergedQueue chan struct{}
	resultQueue chan struct{}
	txResults   []*txResult
	gp          uint64
}

type txResult struct {
	preID   int32
	ID      int32
	st      *state.StateDB
	index   int
	receipt *types.Receipt
}

type indexInfo struct {
	blockIndex int
	txIndex    int
}

func uint64ToBytes(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

func PreCache(bc *BlockChain, st *state.StateDB, number uint64) {
	data, err := state.AccessListDB.Get(uint64ToBytes(number))
	if len(data) == 0 || err != nil {
		return
	}

	list := make([][]byte, 0)
	if err := json.Unmarshal(data, &list); err != nil {
		panic(err)
	}

	batch := 64
	lenList := len(list)
	if lenList == 0 {
		return
	}
	var g errgroup.Group
	for index := 0; index < batch; index++ {
		start := index
		g.Go(func() error {
			for start < lenList {
				key := list[start]
				enc, _ := bc.db.Get(key)
				var value common.Hash
				if len(enc) > 0 {
					_, content, _, err := rlp.Split(enc)
					if err != nil {
						panic(err)
					}
					value.SetBytes(content)
				}
				//fmt.Println("lllllllllll", hex.EncodeToString(key), value.String())
				st.MergedSts.SetStorage(key, value)
				start += batch
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		panic(err)
	}
	fmt.Println("Read list TO db", number, len(list))
}
func NewPallTxManage(blockList types.Blocks, st *state.StateDB, bc *BlockChain) *pallTxManager {
	if !common.NeedStore {
		go PreCache(bc, st, blockList[0].NumberU64())
	}

	fmt.Println("pall", "from", blockList[0].NumberU64(), "to", blockList[len(blockList)-1].NumberU64())
	errCnt = 0
	txLen := 0
	gp := uint64(0)

	mpToRealIndex := make([]*indexInfo, 0)

	fromList := make([]common.Address, 0)
	toList := make([]*common.Address, 0)

	minerAndUncle := make([]map[common.Address]bool, 0)
	for blockIndex, block := range blockList {
		signer := types.MakeSigner(bc.chainConfig, block.Number())
		for tIndex, tx := range block.Transactions() {
			sender, _ := types.Sender(signer, tx)
			fromList = append(fromList, sender)
			toList = append(toList, tx.To())
			mpToRealIndex = append(mpToRealIndex, &indexInfo{
				blockIndex: blockIndex,
				txIndex:    tIndex,
			})
		}
		txLen += len(block.Transactions())
		gp += block.GasLimit()

		types.AddCacheHeader(block.Header())

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
	groupInfo, headTxInGroup, groupLen := newGroupInfo(fromList, toList)
	p := &pallTxManager{
		//pending:        make([]bool, txLen, txLen),
		pending:        make([]int32, txLen, txLen),
		needFailed:     make([]bool, txLen, txLen),
		blocks:         blockList,
		minersAndUncle: minerAndUncle,

		indexInfos: mpToRealIndex,

		txLen: txLen,
		bc:    bc,

		groupInfo:      groupInfo,
		baseStateDB:    st,
		mergedReceipts: make([]*types.Receipt, txLen, txLen),
		ch:             make(chan struct{}, 1),

		txQueue:     make(chan int, txLen),
		resultQueue: make(chan struct{}, txLen),
		txResults:   make([]*txResult, txLen, txLen),
		gp:          gp,
	}

	for _, txIndex := range headTxInGroup {
		p.txQueue <- txIndex
	}

	if len(blockList[0].Transactions()) == 0 {
		p.calReward(0, 0)
	}

	if txLen == 0 {
		p.baseStateDB.FinalUpdateObjs(blockList[0].NumberU64())
		return p
	}

	thread := groupLen
	if thread > 32 {
		thread = 32
	}

	for index := 0; index < thread; index++ {
		go p.txLoop()
	}
	go p.mergeLoop()
	go p.sbLoop()
	return p
}

func (p *pallTxManager) sbLoop() {
	start := -1
	for !p.ended {
		time.Sleep(500 * time.Millisecond)
		if start == p.baseStateDB.MergedIndex {
			fmt.Println("sb----", p.baseStateDB.MergedIndex+1)
			p.txQueue <- p.baseStateDB.MergedIndex + 1
			fmt.Println("sb----end", p.baseStateDB.MergedIndex+1)
		} else {
			start = p.baseStateDB.MergedIndex
		}
	}
}

func (p *pallTxManager) isPending(index int) bool {
	return atomic.LoadInt32(&p.pending[index]) == 1
}

func (p *pallTxManager) setPending(index int, stats bool) {
	if stats {
		atomic.StoreInt32(&p.pending[index], 1)
	} else {
		atomic.StoreInt32(&p.pending[index], 0)
	}

}

func (p *pallTxManager) getResultID() int32 {
	atomic.AddInt32(&p.resultID, 1)
	return p.resultID
}

func (p *pallTxManager) calReward(blockIndex int, txIndex int) {
	p.blockFinalize(blockIndex, txIndex)
	for index := blockIndex + 1; index < len(p.blocks); index++ {
		if len(p.blocks[index].Transactions()) == 0 {
			p.blockFinalize(index, txIndex)
		} else {
			return
		}
	}
}

func (p *pallTxManager) blockFinalize(blockIndex int, txIndex int) {
	block := p.blocks[blockIndex]
	p.bc.engine.Finalize(p.bc, block.Header(), p.baseStateDB, block.Transactions(), block.Uncles())
	if block.NumberU64() == p.bc.Config().DAOForkBlock.Uint64()-1 {
		misc.ApplyDAOHardFork(p.baseStateDB)
	}

	p.baseStateDB.MergeReward(txIndex)
}

func (p *pallTxManager) AddReceiptToQueue(re *txResult) bool {
	if re == nil {
		return false
	}
	if p.needFailed[re.index] {
		p.needFailed[re.index] = false
		//fmt.Println("can not save", re.index)
		return false
	}

	if p.txResults[re.index] == nil {
		p.markNextFailed(re.index)
		re.ID = p.getResultID()
		p.txResults[re.index] = re
		fmt.Println("set---result", re.index, time.Now().String())
		if nextTxIndex, ok := p.groupInfo.nextTxInGroup[re.index]; ok {
			//fmt.Println("nexxxxxxxxxxxxxxxxx", re.index, nextTxIndex)
			p.push(nextTxIndex)
			//fmt.Println("nexxxxxxxxxxxxxxxxx-end", re.index, nextTxIndex)
		}
		if len(p.resultQueue) != p.txLen {
			//fmt.Println("set---", re.index)
			p.resultQueue <- struct{}{}
		}
		return true
	} else {
		//fmt.Println("already have resulet", re.index)
		return true
	}

}

func (p *pallTxManager) txLoop() {
	for !p.ended {
		txIndex, ok := <-p.txQueue
		if !ok {
			break
		}
		//fmt.Println("txLoop", txIndex, p.pending[txIndex], p.txResults[txIndex] != nil)
		if p.txResults[txIndex] != nil {
			p.setPending(txIndex, false)
			continue
		}
		re := p.handleTx(txIndex)
		//fmt.Println("handle tx end", txIndex, p.baseStateDB.MergedIndex)
		p.setPending(txIndex, false)
		stats := p.AddReceiptToQueue(re)
		if stats {
		} else {
			if txIndex > p.baseStateDB.MergedIndex {
				//fmt.Println("push-1", txIndex)
				p.push(txIndex)
				//fmt.Println("push-2", txIndex)
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
		handled := false

		nextTx := p.baseStateDB.MergedIndex + 1
		for nextTx < p.txLen && p.txResults[nextTx] != nil {
			rr := p.txResults[nextTx]
			fmt.Println("handle receipt", "fake", rr.preID, "index", rr.index, "当前base", p.baseStateDB.MergedIndex, "基于", rr.st.MergedIndex, "区块", p.blocks[p.indexInfos[rr.index].blockIndex].NumberU64(), "real tx", p.indexInfos[rr.index].txIndex, "seed", rr.ID, time.Now().String())

			handled = true
			if succ := p.handleReceipt(rr); !succ {
				fmt.Println("handle receipt failed", rr.index, time.Now().String())
				p.markNextFailed(rr.index)
				p.txResults[rr.index] = nil
				break
			}

			if p.indexInfos[rr.index].txIndex == len(p.blocks[p.indexInfos[rr.index].blockIndex].Transactions())-1 {
				p.calReward(p.indexInfos[rr.index].blockIndex, rr.index)
			}
			fmt.Println("MMMMMMMMMMM", nextTx, time.Now().String())
			p.baseStateDB.MergedIndex = nextTx
			nextTx = p.baseStateDB.MergedIndex + 1
		}

		if p.baseStateDB.MergedIndex+1 == p.txLen && !p.ended {
			p.ended = true
			p.baseStateDB.FinalUpdateObjs(p.blocks[0].NumberU64())
			close(p.txQueue)
			//close(p.resultQueue)
			p.ch <- struct{}{}
			//fmt.Println("finial block")
			return
		}
		//if handled {
		//fmt.Println("====================================", p.baseStateDB.MergedIndex+1)
		p.push(p.baseStateDB.MergedIndex + 1)
		//fmt.Println("====================================-end", p.baseStateDB.MergedIndex+1)
		//}
		fmt.Println("mergeLoop---end", p.baseStateDB.MergedIndex, "lenQueue", len(p.resultQueue), handled, time.Now().String())
	}
}

func (p *pallTxManager) markNextFailed(next int) {
	for true {
		var ok bool
		next, ok = p.groupInfo.nextTxInGroup[next]
		if !ok {
			break
		}
		if p.txResults[next] != nil {
			p.txResults[next] = nil
		} else {
			if p.isPending(next) {
				p.needFailed[next] = true
			}
			break
		}
	}
}
func (p *pallTxManager) handleReceipt(rr *txResult) bool {
	if rr.preID != -1 && rr.preID != p.txResults[rr.st.MergedIndex].ID {
		//fmt.Println("?>>>>>>>>>>>>>>>>>>>>", rr.index)
		return false
	}

	blockIndex := p.indexInfos[rr.index].blockIndex
	txIndex := p.indexInfos[rr.index].txIndex
	block := p.blocks[blockIndex]
	if rr.receipt != nil && !rr.st.Conflict(p.baseStateDB, p.minersAndUncle[blockIndex], rr.preID != -1, p.groupInfo.indexToGroupID) {
		txFee := new(big.Int).Mul(new(big.Int).SetUint64(rr.receipt.GasUsed), block.Transactions()[txIndex].GasPrice())
		rr.st.Merge(p.baseStateDB, block.Coinbase(), txFee)
		p.gp -= rr.receipt.GasUsed
		p.mergedReceipts[rr.index] = rr.receipt
		return true
	}
	//fmt.Println("????????????-2", rr.index)
	return false
}

var (
	errCnt = 0
)

func (p *pallTxManager) handleTx(index int) *txResult {
	block := p.blocks[p.indexInfos[index].blockIndex]
	txRealIndex := p.indexInfos[index].txIndex
	tx := block.Transactions()[txRealIndex]

	var st *state.StateDB

	preResultID := int32(-1)
	preIndex, existPre := p.groupInfo.preTxInGroup[index]

	preResult := p.txResults[preIndex]
	if existPre && preResult != nil && preIndex > p.baseStateDB.MergedIndex {
		st = preResult.st.Copy()
		st.MergedIndex = preIndex
		preResultID = preResult.ID

	} else {
		st, _ = state.New(common.Hash{}, p.bc.stateCache, p.bc.snaps)
		st.MergedIndex = p.baseStateDB.MergedIndex
	}

	st.MergedSts = p.baseStateDB.MergedSts
	gas := p.gp

	st.Prepare(tx.Hash(), block.Hash(), txRealIndex)
	st.IndexInAllBlock = index
	if p.txResults[index] != nil || index <= p.baseStateDB.MergedIndex {
		//fmt.Println("???????????-1", index, p.txResults[index] != nil, p.baseStateDB.MergedIndex)
		return nil
	}

	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, new(GasPool).AddGas(gas), st, block.Header(), tx, nil, p.bc.vmConfig)
	fmt.Println("end apply tx", "useFake", preResultID, "执行", index, "基于", st.MergedIndex, "当前base", p.baseStateDB.MergedIndex, "blockIndex", p.blocks[p.indexInfos[index].blockIndex].NumberU64(), "realIndex", p.indexInfos[index].txIndex, err)

	if index <= p.baseStateDB.MergedIndex {
		//fmt.Println("???????????-2", index, p.baseStateDB.MergedIndex)
		return nil
	}
	if err != nil && st.MergedIndex+1 == index && st.MergedIndex == p.baseStateDB.MergedIndex && preResultID == -1 {
		errCnt++
		if errCnt > 100 {
			fmt.Println("?????????", st.MergedIndex, index, p.baseStateDB.MergedIndex, preResultID)
			fmt.Println("sbbbbbbbbbbbb", "useFake", preResultID, "执行", index, "基于", st.MergedIndex, "当前base", p.baseStateDB.MergedIndex, "blockIndex", p.blocks[p.indexInfos[index].blockIndex].NumberU64(), "realIndex", p.indexInfos[index].txIndex)
			panic(err)
		}
	}

	return &txResult{
		preID:   preResultID,
		st:      st,
		index:   index,
		receipt: receipt,
	}
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
