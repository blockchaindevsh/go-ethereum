package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
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
}

func NewPallTxManage(block *types.Block, st *state.StateDB, bc *BlockChain) *pallTxManage {
	p := &pallTxManage{
		block:       block,
		baseStateDB: st,
		bc:          bc,
		gp:          new(GasPool).AddGas(block.GasLimit()),
		ch:          make(chan struct{}, 1),
	}
	if len(block.Transactions()) == 0 {
		p.ch <- struct{}{}
	}
	return p
}

func (p *pallTxManage) AddTx(tx *types.Transaction, txIndex int) {
	st := p.baseStateDB.Copy()
	st.Prepare(tx.Hash(), p.block.Hash(), txIndex)
	receipt, err := ApplyTransaction(p.bc.chainConfig, p.bc, nil, p.gp, st, p.block.Header(), tx, nil, p.bc.vmConfig)
	if err != nil {
		panic(err)
	}
	p.receipts[txIndex] = receipt
	p.baseStateDB = st
	if txIndex == len(p.block.Transactions())-1 {
		p.Done()
	}
}

func (p *pallTxManage) GetReceiptsAndLogs() (types.Receipts, []*types.Log) {
	rs := make(types.Receipts, 0)
	logs := make([]*types.Log, 0)
	txLen := len(p.block.Transactions())
	all := uint64(0)
	for index := 0; index < txLen; index++ {
		p.receipts[index].CumulativeGasUsed = all + p.receipts[index].GasUsed
		rs = append(rs, p.receipts[index])
		logs = append(logs, p.receipts[index].Logs...)
	}
	return rs, logs
}

func (p *pallTxManage) Done() {
	p.ch <- struct{}{}
	fmt.Println("Set done ", p.block.NumberU64())
}
