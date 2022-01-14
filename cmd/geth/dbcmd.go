// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	st "github.com/golang-collections/collections/stack"
	"gopkg.in/urfave/cli.v1"
)

var (
	removedbCommand = cli.Command{
		Action:    utils.MigrateFlags(removeDB),
		Name:      "removedb",
		Usage:     "Remove blockchain and state databases",
		ArgsUsage: "",
		Flags: []cli.Flag{
			utils.DataDirFlag,
		},
		Category: "DATABASE COMMANDS",
		Description: `
Remove blockchain and state databases`,
	}
	dbCommand = cli.Command{
		Name:      "db",
		Usage:     "Low level database operations",
		ArgsUsage: "",
		Category:  "DATABASE COMMANDS",
		Subcommands: []cli.Command{
			dbInspectCmd,
			dbStatCmd,
			dbCompactCmd,
			dbGetCmd,
			dbDeleteCmd,
			dbPutCmd,
			dbGetSlotsCmd,
			dbDumpFreezerIndex,
			dbImportCmd,
			dbExportCmd,
		},
	}
	dbInspectCmd = cli.Command{
		Action:    utils.MigrateFlags(inspect),
		Name:      "inspect",
		ArgsUsage: "<prefix> <start>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.AncientFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Usage:       "Inspect the storage size for each type of data in the database",
		Description: `This commands iterates the entire database. If the optional 'prefix' and 'start' arguments are provided, then the iteration is limited to the given subset of data.`,
	}
	dbStatCmd = cli.Command{
		Action: utils.MigrateFlags(dbStats),
		Name:   "stats",
		Usage:  "Print leveldb statistics",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
	}
	dbCompactCmd = cli.Command{
		Action: utils.MigrateFlags(dbCompact),
		Name:   "compact",
		Usage:  "Compact leveldb database. WARNING: May take a very long time",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
			utils.CacheFlag,
			utils.CacheDatabaseFlag,
		},
		Description: `This command performs a database compaction. 
WARNING: This operation may take a very long time to finish, and may cause database
corruption if it is aborted during execution'!`,
	}
	dbGetCmd = cli.Command{
		Action:    utils.MigrateFlags(dbGet),
		Name:      "get",
		Usage:     "Show the value of a database key",
		ArgsUsage: "<hex-encoded key>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: "This command looks up the specified database key from the database.",
	}
	dbDeleteCmd = cli.Command{
		Action:    utils.MigrateFlags(dbDelete),
		Name:      "delete",
		Usage:     "Delete a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: `This command deletes the specified database key from the database. 
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbPutCmd = cli.Command{
		Action:    utils.MigrateFlags(dbPut),
		Name:      "put",
		Usage:     "Set the value of a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key> <hex-encoded value>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: `This command sets a given database key to the given value. 
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbGetSlotsCmd = cli.Command{
		Action:    utils.MigrateFlags(dbDumpTrie),
		Name:      "dumptrie",
		Usage:     "Show the storage key/values of a given storage trie",
		ArgsUsage: "<hex-encoded storage trie root> <hex-encoded start (optional)> <int max elements (optional)>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: "This command looks up the specified database key from the database.",
	}
	dbDumpFreezerIndex = cli.Command{
		Action:    utils.MigrateFlags(freezerInspect),
		Name:      "freezer-index",
		Usage:     "Dump out the index of a given freezer type",
		ArgsUsage: "<type> <start (int)> <end (int)>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.SepoliaFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: "This command displays information about the freezer index.",
	}
	dbImportCmd = cli.Command{
		Action:    utils.MigrateFlags(importLDBdata),
		Name:      "import",
		Usage:     "Imports leveldb-data from an exported RLP dump.",
		ArgsUsage: "<dumpfile> <start (optional)",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: "The import command imports the specific chain data from an RLP encoded stream.",
	}
	dbExportCmd = cli.Command{
		Action:    utils.MigrateFlags(exportChaindata),
		Name:      "export",
		Usage:     "Exports the chain data into an RLP dump. If the <dumpfile> has .gz suffix, gzip compression will be used.",
		ArgsUsage: "<type> <dumpfile>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.MainnetFlag,
			utils.RopstenFlag,
			utils.RinkebyFlag,
			utils.GoerliFlag,
		},
		Description: "Exports the specified chain data to an RLP encoded stream, optionally gzip-compressed.",
	}
)

func removeDB(ctx *cli.Context) error {
	stack, config := makeConfigNode(ctx)

	// Remove the full node state database
	path := stack.ResolvePath("chaindata")
	if common.FileExist(path) {
		confirmAndRemoveDB(path, "full node state database")
	} else {
		log.Info("Full node state database missing", "path", path)
	}
	// Remove the full node ancient database
	path = config.Eth.DatabaseFreezer
	switch {
	case path == "":
		path = filepath.Join(stack.ResolvePath("chaindata"), "ancient")
	case !filepath.IsAbs(path):
		path = config.Node.ResolvePath(path)
	}
	if common.FileExist(path) {
		confirmAndRemoveDB(path, "full node ancient database")
	} else {
		log.Info("Full node ancient database missing", "path", path)
	}
	// Remove the light node database
	path = stack.ResolvePath("lightchaindata")
	if common.FileExist(path) {
		confirmAndRemoveDB(path, "light node database")
	} else {
		log.Info("Light node database missing", "path", path)
	}
	return nil
}

// confirmAndRemoveDB prompts the user for a last confirmation and removes the
// folder if accepted.
func confirmAndRemoveDB(database string, kind string) {
	confirm, err := prompt.Stdin.PromptConfirm(fmt.Sprintf("Remove %s (%s)?", kind, database))
	switch {
	case err != nil:
		utils.Fatalf("%v", err)
	case !confirm:
		log.Info("Database deletion skipped", "path", database)
	default:
		start := time.Now()
		filepath.Walk(database, func(path string, info os.FileInfo, err error) error {
			// If we're at the top level folder, recurse into
			if path == database {
				return nil
			}
			// Delete all the files, but not subfolders
			if !info.IsDir() {
				os.Remove(path)
				return nil
			}
			return filepath.SkipDir
		})
		log.Info("Database successfully deleted", "path", database, "elapsed", common.PrettyDuration(time.Since(start)))
	}
}

func inspect(ctx *cli.Context) error {
	var (
		prefix []byte
		start  []byte
	)
	if ctx.NArg() > 2 {
		return fmt.Errorf("Max 2 arguments: %v", ctx.Command.ArgsUsage)
	}
	if ctx.NArg() >= 1 {
		if d, err := hexutil.Decode(ctx.Args().Get(0)); err != nil {
			return fmt.Errorf("failed to hex-decode 'prefix': %v", err)
		} else {
			prefix = d
		}
	}
	if ctx.NArg() >= 2 {
		if d, err := hexutil.Decode(ctx.Args().Get(1)); err != nil {
			return fmt.Errorf("failed to hex-decode 'start': %v", err)
		} else {
			start = d
		}
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	return rawdb.InspectDatabase(db, prefix, start)
}

func showLeveldbStats(db ethdb.Stater) {
	if stats, err := db.Stat("leveldb.stats"); err != nil {
		log.Warn("Failed to read database stats", "error", err)
	} else {
		fmt.Println(stats)
	}
	if ioStats, err := db.Stat("leveldb.iostats"); err != nil {
		log.Warn("Failed to read database iostats", "error", err)
	} else {
		fmt.Println(ioStats)
	}
}

func dbStats(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	showLeveldbStats(db)
	return nil
}

func dbCompact(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	log.Info("Stats before compaction")
	showLeveldbStats(db)

	log.Info("Triggering compaction")
	if err := db.Compact(nil, nil); err != nil {
		log.Info("Compact err", "error", err)
		return err
	}
	log.Info("Stats after compaction")
	showLeveldbStats(db)
	return nil
}

// dbGet shows the value of a given database key
func dbGet(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	key, err := parseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}

	data, err := db.Get(key)
	if err != nil {
		log.Info("Get operation failed", "key", fmt.Sprintf("0x%#x", key), "error", err)
		return err
	}
	fmt.Printf("key %#x: %#x\n", key, data)
	return nil
}

// dbDelete deletes a key from the database
func dbDelete(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	key, err := parseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}
	data, err := db.Get(key)
	if err == nil {
		fmt.Printf("Previous value: %#x\n", data)
	}
	if err = db.Delete(key); err != nil {
		log.Info("Delete operation returned an error", "key", fmt.Sprintf("0x%#x", key), "error", err)
		return err
	}
	return nil
}

// dbPut overwrite a value in the database
func dbPut(ctx *cli.Context) error {
	if ctx.NArg() != 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	var (
		key   []byte
		value []byte
		data  []byte
		err   error
	)
	key, err = parseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}
	value, err = hexutil.Decode(ctx.Args().Get(1))
	if err != nil {
		log.Info("Could not decode the value", "error", err)
		return err
	}
	data, err = db.Get(key)
	if err == nil {
		fmt.Printf("Previous value: %#x\n", data)
	}
	return db.Put(key, value)
}

type dumpTask struct {
	root    common.Hash
	begin   []byte // inclusive
	end     []byte // exclusive
	isWorld bool
}

// dbDumpTrie shows the key-value slots of a given storage trie
func dbDumpTrie(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()
	var (
		root []byte
		// start []byte
		// max = int64(-1)
		err     error
		nthread = int(1)
	)
	if root, err = hexutil.Decode(ctx.Args().Get(0)); err != nil {
		log.Info("Could not decode the root", "error", err)
		return err
	}
	stRoot := common.BytesToHash(root)
	if ctx.NArg() >= 2 {
		if nthread, err = strconv.Atoi(ctx.Args().Get(1)); err != nil {
			log.Info("Could not decode the seek position", "error", err)
			return err
		}
	}
	// if ctx.NArg() >= 3 {
	// 	if max, err = strconv.ParseInt(ctx.Args().Get(2), 10, 64); err != nil {
	// 		log.Info("Could not decode the max count", "error", err)
	// 		return err
	// 	}
	// }

	var mu sync.Mutex
	var wg sync.WaitGroup

	s := st.New()

	wg.Add(nthread)
	for i := 0; i < nthread; i++ {
		if i != nthread-1 {
			s.Push(dumpTask{
				root:    stRoot,
				begin:   []byte{byte(256 / nthread * i)},
				end:     []byte{byte(256 / nthread * (i + 1))},
				isWorld: true,
			})
		} else {
			s.Push(dumpTask{
				root:    stRoot,
				begin:   []byte{byte(256 / nthread * i)},
				end:     nil,
				isWorld: true,
			})
		}
	}

	startTime := time.Now()
	var size common.StorageSize
	var accountCount int64
	var storageCount int64

	log.Info("KV iterator", "nthread", nthread)

	for i := 0; i < nthread; i++ {
		go func() {
			for {
				var t dumpTask

				mu.Lock()
				if s.Len() == 0 {
					mu.Unlock()
					break
				}
				t = s.Pop().(dumpTask)
				mu.Unlock()

				log.Info("Iterating root", "root", t.root, "start", t.begin, "end", t.end)
				theTrie, err := trie.New(t.root, trie.NewDatabase(db))
				if err != nil {
					log.Error("Failed to open root trie", "err", err)
					break
				}

				logged := time.Now()
				it := trie.NewIterator(theTrie.NodeIterator(t.begin))
				var rootCount int64
				var rootSize common.StorageSize
				var count int64

				for it.Next() {
					isDone := false
					for i := 0; i < len(t.end); i++ {
						if it.Key[i] >= t.end[i] {
							isDone = true
							break
						}
					}
					if isDone {
						break
					}
					rootCount++
					rootSize += common.StorageSize(len(it.Key) + len(it.Value))
					// fmt.Printf("  %d. key %#x: %#x\n", count, it.Key, it.Value)
					count++
					if count%1000 == 0 && time.Since(logged) > 8*time.Second {
						mu.Lock()
						size += rootSize
						if t.isWorld {
							accountCount += rootCount
						} else {
							storageCount += rootCount
						}

						rootSize = 0
						rootCount = 0
						mu.Unlock()

						log.Info("Iterating kv", "acc_count", accountCount, "storage_count", storageCount, "size", size, "pending roots", s.Len(), "elapsed", common.PrettyDuration(time.Since(startTime)))
						logged = time.Now()
					}

					if t.isWorld {
						data := new(types.StateAccount)
						if err := rlp.DecodeBytes(it.Value, data); err != nil {
							log.Error("Failed to decode state object", "addr", it.Key, "err", err)
							return
						}

						if data.Root != emptyRoot {
							mu.Lock()
							s.Push(dumpTask{
								root:    data.Root,
								begin:   nil,
								end:     nil,
								isWorld: false,
							})
							mu.Unlock()
						}
					}
				}

				mu.Lock()
				size += rootSize
				if t.isWorld {
					accountCount += rootCount
				} else {
					storageCount += rootCount
				}

				rootSize = 0
				rootCount = 0
				mu.Unlock()

				log.Info("Iterated root", "root", t.root, "start", t.begin, "end", t.end)
			}

			wg.Done()
		}()
	}

	wg.Wait()

	log.Info("Done", "acc_count", accountCount, "storage_count", storageCount, "size", size, "elapsed", common.PrettyDuration(time.Since(startTime)))
	return nil
}

func freezerInspect(ctx *cli.Context) error {
	var (
		start, end    int64
		disableSnappy bool
		err           error
	)
	if ctx.NArg() < 3 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	kind := ctx.Args().Get(0)
	if noSnap, ok := rawdb.FreezerNoSnappy[kind]; !ok {
		var options []string
		for opt := range rawdb.FreezerNoSnappy {
			options = append(options, opt)
		}
		sort.Strings(options)
		return fmt.Errorf("Could read freezer-type '%v'. Available options: %v", kind, options)
	} else {
		disableSnappy = noSnap
	}
	if start, err = strconv.ParseInt(ctx.Args().Get(1), 10, 64); err != nil {
		log.Info("Could read start-param", "error", err)
		return err
	}
	if end, err = strconv.ParseInt(ctx.Args().Get(2), 10, 64); err != nil {
		log.Info("Could read count param", "error", err)
		return err
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()
	path := filepath.Join(stack.ResolvePath("chaindata"), "ancient")
	log.Info("Opening freezer", "location", path, "name", kind)
	if f, err := rawdb.NewFreezerTable(path, kind, disableSnappy); err != nil {
		return err
	} else {
		f.DumpIndex(start, end)
	}
	return nil
}

// ParseHexOrString tries to hexdecode b, but if the prefix is missing, it instead just returns the raw bytes
func parseHexOrString(str string) ([]byte, error) {
	b, err := hexutil.Decode(str)
	if errors.Is(err, hexutil.ErrMissingPrefix) {
		return []byte(str), nil
	}
	return b, err
}

func importLDBdata(ctx *cli.Context) error {
	start := 0
	switch ctx.NArg() {
	case 1:
		break
	case 2:
		s, err := strconv.Atoi(ctx.Args().Get(1))
		if err != nil {
			return fmt.Errorf("second arg must be an integer: %v", err)
		}
		start = s
	default:
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	var (
		fName     = ctx.Args().Get(0)
		stack, _  = makeConfigNode(ctx)
		interrupt = make(chan os.Signal, 1)
		stop      = make(chan struct{})
	)
	defer stack.Close()
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during ldb import, stopping at next batch")
		}
		close(stop)
	}()
	db := utils.MakeChainDatabase(ctx, stack, false)
	return utils.ImportLDBData(db, fName, int64(start), stop)
}

type preimageIterator struct {
	iter ethdb.Iterator
}

func (iter *preimageIterator) Next() (byte, []byte, []byte, bool) {
	for iter.iter.Next() {
		key := iter.iter.Key()
		if bytes.HasPrefix(key, rawdb.PreimagePrefix) && len(key) == (len(rawdb.PreimagePrefix)+common.HashLength) {
			return utils.OpBatchAdd, key, iter.iter.Value(), true
		}
	}
	return 0, nil, nil, false
}

func (iter *preimageIterator) Release() {
	iter.iter.Release()
}

type snapshotIterator struct {
	init    bool
	account ethdb.Iterator
	storage ethdb.Iterator
}

func (iter *snapshotIterator) Next() (byte, []byte, []byte, bool) {
	if !iter.init {
		iter.init = true
		return utils.OpBatchDel, rawdb.SnapshotRootKey, nil, true
	}
	for iter.account.Next() {
		key := iter.account.Key()
		if bytes.HasPrefix(key, rawdb.SnapshotAccountPrefix) && len(key) == (len(rawdb.SnapshotAccountPrefix)+common.HashLength) {
			return utils.OpBatchAdd, key, iter.account.Value(), true
		}
	}
	for iter.storage.Next() {
		key := iter.storage.Key()
		if bytes.HasPrefix(key, rawdb.SnapshotStoragePrefix) && len(key) == (len(rawdb.SnapshotStoragePrefix)+2*common.HashLength) {
			return utils.OpBatchAdd, key, iter.storage.Value(), true
		}
	}
	return 0, nil, nil, false
}

func (iter *snapshotIterator) Release() {
	iter.account.Release()
	iter.storage.Release()
}

// chainExporters defines the export scheme for all exportable chain data.
var chainExporters = map[string]func(db ethdb.Database) utils.ChainDataIterator{
	"preimage": func(db ethdb.Database) utils.ChainDataIterator {
		iter := db.NewIterator(rawdb.PreimagePrefix, nil)
		return &preimageIterator{iter: iter}
	},
	"snapshot": func(db ethdb.Database) utils.ChainDataIterator {
		account := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
		storage := db.NewIterator(rawdb.SnapshotStoragePrefix, nil)
		return &snapshotIterator{account: account, storage: storage}
	},
}

func exportChaindata(ctx *cli.Context) error {
	if ctx.NArg() < 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	// Parse the required chain data type, make sure it's supported.
	kind := ctx.Args().Get(0)
	kind = strings.ToLower(strings.Trim(kind, " "))
	exporter, ok := chainExporters[kind]
	if !ok {
		var kinds []string
		for kind := range chainExporters {
			kinds = append(kinds, kind)
		}
		return fmt.Errorf("invalid data type %s, supported types: %s", kind, strings.Join(kinds, ", "))
	}
	var (
		stack, _  = makeConfigNode(ctx)
		interrupt = make(chan os.Signal, 1)
		stop      = make(chan struct{})
	)
	defer stack.Close()
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during db export, stopping at next batch")
		}
		close(stop)
	}()
	db := utils.MakeChainDatabase(ctx, stack, true)
	return utils.ExportChaindata(ctx.Args().Get(1), kind, exporter(db), stop)
}
