package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gansidui/priority_queue"
	"time"
)

type pallTxManage struct {
	block           *types.Block
	bc              *BlockChain
	baseStateDB     *state.StateDB
	currentIndex    int
	currentReadMap  map[common.Address]struct{}
	currentWriteMap map[common.Address]struct{}
	gp              *GasPool
	receipts        map[int]*types.Receipt
	ch              chan struct{}
	txList          chan *TxWithIndex
	ququeTx         *priority_queue.PriorityQueue
	ququeRe         *priority_queue.PriorityQueue

	metged bool
}
type TxWithIndex struct {
	tx      *types.Transaction
	txIndex int
}

type Re struct {
	st      *state.StateDB
	txIndex int
	receipt *types.Receipt
}

func (this *TxWithIndex) Less(other interface{}) bool {
	return this.txIndex < other.(*TxWithIndex).txIndex
}

func (this *Re) Less(other interface{}) bool {
	return this.txIndex < other.(*Re).txIndex
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManage {
	if len(block.Transactions()) == 0 {
		return nil
	}
	p := &pallTxManage{
		block:       block,
		baseStateDB: st,
		bc:          bc,
		gp:          new(GasPool).AddGas(block.GasLimit()),
		receipts:    make(map[int]*types.Receipt, 0),
		ch:          make(chan struct{}, 1),
		txList:      make(chan *TxWithIndex, 0),
		ququeTx:     priority_queue.New(),
		ququeRe:     priority_queue.New(),
	}
	for index := 0; index < 1; index++ {
		go p.txLoop()
	}

	go p.mergeLoop()

	return p
}

func (p *pallTxManage) AddTx(tx *types.Transaction, txIndex int) {
	p.ququeTx.Push(&TxWithIndex{tx: tx, txIndex: txIndex})
}
func (p *pallTxManage) GetTx() *TxWithIndex {
	if p.ququeTx.Len() == 0 {
		return nil
	}
	return p.ququeTx.Pop().(*TxWithIndex)
}

func (p *pallTxManage) AddRe(re *Re) {
	p.ququeRe.Push(re)
}
func (p *pallTxManage) GetRe() *Re {
	if p.ququeRe.Len() == 0 {
		return nil
	}
	return p.ququeRe.Pop().(*Re)
}

func (p *pallTxManage) txLoop() {
	for {
		tt := p.GetTx()
		if tt != nil {
			p.handleTx(tt.tx, tt.txIndex)
		} else {
			continue
		}
		if p.metged {
			fmt.Println("already merged")
			return
		}

	}
}

func (p *pallTxManage) mergeLoop() {
	for {
		rr := p.GetRe()
		if rr == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		//fmt.Println("mergeloop", p.baseStateDB.CurrMergedNumber, rr.txIndex, rr.st.CanMerge(p.baseStateDB))
		if rr.st.CanMerge(p.baseStateDB) { //merged
			rr.st.Merge(p.baseStateDB)
		}
		if p.baseStateDB.CurrMergedNumber == len(p.block.Transactions())-1 {
			//panic("=====")
			p.markEnd()
			//fmt.Println("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
		}

	}
}

func (p *pallTxManage) handleTx(tx *types.Transaction, txIndex int) {
	st := p.baseStateDB.Copy()
	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, p.gp, st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		p.AddTx(tx, txIndex)
		return
	}

	p.AddRe(&Re{
		st:      st,
		txIndex: txIndex,
		receipt: receipt,
	})
	p.receipts[txIndex] = receipt
}

func (p *pallTxManage) markEnd() {
	p.metged = true
	p.ch <- struct{}{}
}

func (p *pallTxManage) GetReceiptsAndLogs() (types.Receipts, []*types.Log, uint64) {
	rs := make(types.Receipts, 0)
	logs := make([]*types.Log, 0)
	txLen := len(p.block.Transactions())
	all := uint64(0)
	for index := 0; index < txLen; index++ {
		all = all + p.receipts[index].GasUsed
		p.receipts[index].CumulativeGasUsed = all
		p.receipts[index].Bloom = types.CreateBloom(types.Receipts{p.receipts[index]})
		rs = append(rs, p.receipts[index])
		logs = append(logs, p.receipts[index].Logs...)
		//fmt.Println("-------end-----------", "blockNumber", p.block.NumberU64(), "txIndex", index, p.receipts[index].GasUsed)
	}
	return rs, logs, all
}

func (p *pallTxManage) Done() {
	p.ch <- struct{}{}
	//fmt.Println("Set done ", p.block.NumberU64())
}
