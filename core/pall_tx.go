package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gansidui/priority_queue"
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
	quque           *priority_queue.PriorityQueue
}
type TxWithIndex struct {
	tx      *types.Transaction
	txIndex int
}

func (this *TxWithIndex) Less(other interface{}) bool {
	return this.txIndex < other.(*TxWithIndex).txIndex
}
func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManage {
	st.CurrMergedNumber = -1

	p := &pallTxManage{
		block:       block,
		baseStateDB: st,
		bc:          bc,
		gp:          new(GasPool).AddGas(block.GasLimit()),
		receipts:    make(map[int]*types.Receipt, 0),
		ch:          make(chan struct{}, 1),
		txList:      make(chan *TxWithIndex, 0),
		quque:       priority_queue.New(),
	}
	if len(block.Transactions()) == 0 {
		p.ch <- struct{}{}
	}
	for index := 0; index < 16; index++ {
		go p.listen()
	}
	return p
}

func (p *pallTxManage) AddTx(tx *types.Transaction, txIndex int) {
	p.quque.Push(&TxWithIndex{tx: tx, txIndex: txIndex})
}
func (p *pallTxManage) GetTx() *TxWithIndex {
	return p.quque.Pop().(*TxWithIndex)
}
func (p *pallTxManage) loop() {

}
func (p *pallTxManage) listen() {
	for {
		tt := p.GetTx()
		if tt != nil {
			p.handleTx(tt.tx, tt.txIndex)
		} else {
			continue
		}
		if p.ch == nil { //TODO:判断是否关闭
			return
		}

	}
}

func (p *pallTxManage) handleTx(tx *types.Transaction, txIndex int) {
	st := p.baseStateDB.Copy()
	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	fmt.Println("handle tx", tx.Hash().String(), txIndex, st.CurrMergedNumber)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, p.gp, st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		fmt.Println("err", err, p.block.Number(), txIndex, tx.Hash().String())
		panic(err)
	}
	p.receipts[txIndex] = receipt
	if common.PrintData {
		fmt.Println("handle tx", p.block.NumberU64(), receipt.GasUsed)
	}

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
	}
	return rs, logs, all
}

func (p *pallTxManage) Done() {
	p.ch <- struct{}{}
	//fmt.Println("Set done ", p.block.NumberU64())
}
