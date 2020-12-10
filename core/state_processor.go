// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

func (p *StateProcessor) Process(blockList types.Blocks, statedb *state.StateDB, cfg vm.Config) ([]types.Receipts, [][]*types.Log, []uint64, error) {
	var (
		receipts []types.Receipts
		usedGas  []uint64
		//header   = block.Header()
		allLogs [][]*types.Log
	)
	// Mutate the block and state according to any hard-fork specs

	//for _, block := range blockList {
	//	if block.NumberU64() == 3881784+1 {
	//		panic("3881784+1")
	//}
	//}

	pm := NewPallTxManage(blockList, statedb, p.bc)
	if pm.txLen != 0 {
		<-pm.ch
	}

	receipts, allLogs, usedGas = pm.GetReceiptsAndLogs()

	return receipts, allLogs, usedGas, nil
}

var (
	mp = map[string]bool{
		"0x5606b81f1783649f4f2c9dd3b0579e70f0bd631e2ceea90454207ef0d05b46a0": true,
		"": true,
	}
)

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	context := NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, statedb, config, cfg)
	//if mp[tx.Hash().String()]	{
	//	fmt.Println("sssssssssssssssssss",tx.Hash().String(),msg.From().String(),msg.To().String())
	//	vmenv.PrintLog = true
	//}
	//vmenv.PrintLog = true
	// Apply the transaction to the current state (included in the env)
	result, err := ApplyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, err
	}
	// Update the state with pending changes

	statedb.CalReadAndWrite()
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	if usedGas != nil {
		*usedGas += result.UsedGas
	}

	//fmt.Println("执行完毕",tx.Hash().String(),result.UsedGas,err)

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	receipt := types.NewReceipt(root, result.Failed(), 0)
	if usedGas != nil {
		receipt = types.NewReceipt(root, result.Failed(), *usedGas)
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())

	return receipt, err
}
