package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/sstorage"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

var (
	chunkLen  *uint64
	epoch     *int64
	miner     *string
	filenames *[]string

	verbosity *int

	chunkIdx   *uint64
	readLen    *uint64
	readMasked *bool

	shardIdx  *uint64
	kvSize    *uint64
	kvEntries *uint64
	kvIdx     *uint64
)

var CreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a data file",
	Run:   runCreate,
}

var ChunkReadCmd = &cobra.Command{
	Use:   "chunk_read",
	Short: "Read a chunk from a data file",
	Run:   runChunkRead,
}

var ChunkWriteCmd = &cobra.Command{
	Use:   "chunk_write",
	Short: "Write a chunk from a data file",
	Run:   runChunkWrite,
}

var ShardReadCmd = &cobra.Command{
	Use:   "shard_read",
	Short: "Read a KV from a data shard",
	Run:   runShardRead,
}

var ShardWriteCmd = &cobra.Command{
	Use:   "shard_write",
	Short: "Write a value to a data shard",
	Run:   runShardWrite,
}

func init() {
	chunkLen = CreateCmd.Flags().Uint64("len", 0, "Chunk idx len to create")
	epoch = CreateCmd.Flags().Int64("epoch", -1, "mask epoch")

	filenames = rootCmd.PersistentFlags().StringArray("filename", []string{}, "Data filename")
	miner = rootCmd.PersistentFlags().String("miner", "", "miner address")
	verbosity = rootCmd.PersistentFlags().Int("verbosity", 3, "Logging verbosity: 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail")
	chunkIdx = rootCmd.PersistentFlags().Uint64("chunk_idx", 0, "Chunk idx to start/read/write")

	shardIdx = rootCmd.PersistentFlags().Uint64("shard_idx", 0, "Shard idx to read/write")
	kvSize = rootCmd.PersistentFlags().Uint64("kv_size", 4096, "Shard KV size to read/write")
	kvIdx = rootCmd.PersistentFlags().Uint64("kv_idx", 0, "Shard KV index to read/write")
	kvEntries = rootCmd.PersistentFlags().Uint64("kv_entries", 0, "Number of KV entries in the shard")

	readMasked = rootCmd.PersistentFlags().Bool("masked", false, "Read masked or not")
	readLen = rootCmd.PersistentFlags().Uint64("readlen", 0, "Bytes to read (only for unmasked read)")
}

func setupLogger() {
	glogger := log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(false)))
	glogger.Verbosity(log.Lvl(*verbosity))
	log.Root().SetHandler(glogger)

	// setup logger
	var ostream log.Handler
	output := io.Writer(os.Stderr)

	usecolor := (isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	ostream = log.StreamHandler(output, log.TerminalFormat(usecolor))

	glogger.SetHandler(ostream)
}

func runCreate(cmd *cobra.Command, args []string) {
	setupLogger()

	if len(*filenames) != 1 {
		log.Crit("must provide single filename")
	}

	if *epoch < 0 {
		log.Crit("must provide epoch")
	}
	if *miner == "" {
		log.Crit("must provide miner")
	}
	minerAddr := common.HexToAddress(*miner)

	log.Info("Creating data file", "chunkIdx", *chunkIdx, "chunkLen", *chunkLen, "epoch", *epoch, "miner", minerAddr)

	_, err := sstorage.Create((*filenames)[0], *chunkIdx, *chunkLen, uint64(*epoch), *kvSize, minerAddr)
	if err != nil {
		log.Crit("create failed", "error", err)
	}
}

func runChunkRead(cmd *cobra.Command, args []string) {
	setupLogger()

	if len(*filenames) != 1 {
		log.Crit("must provide a filename")
	}

	var err error
	var df *sstorage.DataFile
	df, err = sstorage.OpenDataFile((*filenames)[0])
	if err != nil {
		log.Crit("open failed", "error", err)
	}

	// do not have data hash, use empty hash for placeholder
	b, err := df.Read(*chunkIdx, int(*readLen), common.Hash{}, *readMasked)
	if err != nil {
		log.Crit("open failed", "error", err)
	}
	os.Stdout.Write(b)
}

func readInputBytes() []byte {
	in := bufio.NewReader(os.Stdin)
	b := make([]byte, 0)
	for {
		c, err := in.ReadByte()
		if err == io.EOF {
			break
		}
		b = append(b, c)
	}
	return b
}

func runChunkWrite(cmd *cobra.Command, args []string) {
	setupLogger()

	if len(*filenames) != 1 {
		log.Crit("must provide a filename")
	}

	var err error
	var df *sstorage.DataFile
	df, err = sstorage.OpenDataFile((*filenames)[0])
	if err != nil {
		log.Crit("open failed", "error", err)
	}

	err = df.Write(*chunkIdx, readInputBytes(), false)
	if err != nil {
		log.Crit("write failed", "error", err)
	}
}

func runShardRead(cmd *cobra.Command, args []string) {
	setupLogger()

	ds := sstorage.NewDataShard(*shardIdx, *kvSize, *kvEntries)
	for _, filename := range *filenames {
		var err error
		var df *sstorage.DataFile
		df, err = sstorage.OpenDataFile(filename)
		if err != nil {
			log.Crit("open failed", "error", err)
		}
		ds.AddDataFile(df)
	}

	if !ds.IsComplete() {
		log.Warn("shard is not completed")
	}

	// do not have data hash, use empty hash for placeholder
	b, err := ds.Read(*kvIdx, int(*readLen), common.Hash{}, *readMasked)
	if err != nil {
		log.Crit("read failed", "error", err)
	}
	os.Stdout.Write(b)
}

func runShardWrite(cmd *cobra.Command, args []string) {
	setupLogger()

	ds := sstorage.NewDataShard(*shardIdx, *kvSize, *kvEntries)
	for _, filename := range *filenames {
		var err error
		var df *sstorage.DataFile
		df, err = sstorage.OpenDataFile(filename)
		if err != nil {
			log.Crit("open failed", "error", err)
		}
		ds.AddDataFile(df)
	}

	if !ds.IsComplete() {
		log.Warn("shard is not completed")
	}

	err := ds.Write(*kvIdx, readInputBytes(), false)
	if err != nil {
		log.Crit("write failed", "error", err)
	}
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "sstorage",
	Short: "Sharded storage tools",
}

func init() {
	rootCmd.AddCommand(CreateCmd)
	rootCmd.AddCommand(ChunkReadCmd)
	rootCmd.AddCommand(ChunkWriteCmd)
	rootCmd.AddCommand(ShardReadCmd)
	rootCmd.AddCommand(ShardWriteCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
