package sstorage

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/sstorage/pora"
)

type DataShard struct {
	shardIdx    uint64
	kvSize      uint64
	chunksPerKv uint64
	kvEntries   uint64
	dataFiles   []*DataFile
}

func NewDataShard(shardIdx uint64, kvSize uint64, kvEntries uint64) *DataShard {
	if kvSize%CHUNK_SIZE != 0 {
		panic("kvSize must be CHUNK_SIZE at the moment")
	}

	return &DataShard{shardIdx: shardIdx, kvSize: kvSize, chunksPerKv: kvSize / CHUNK_SIZE, kvEntries: kvEntries}
}

func (ds *DataShard) AddDataFile(df *DataFile) {
	// TODO: May check if not overlapped?
	ds.dataFiles = append(ds.dataFiles, df)
}

// Returns whether the shard has all data files to cover all entries
func (ds *DataShard) IsComplete() bool {
	chunkIdx := ds.StartChunkIdx()
	chunkIdxEnd := (ds.shardIdx + 1) * ds.chunksPerKv * ds.kvEntries
	for chunkIdx < chunkIdxEnd {
		found := false
		for _, df := range ds.dataFiles {
			if df.Contains(chunkIdx) {
				chunkIdx = df.ChunkIdxEnd()
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (ds *DataShard) Contains(kvIdx uint64) bool {
	return kvIdx >= ds.shardIdx*ds.kvEntries && kvIdx < (ds.shardIdx+1)*ds.kvEntries
}

func (ds *DataShard) StartChunkIdx() uint64 {
	return ds.shardIdx * ds.chunksPerKv * ds.kvEntries
}

func (ds *DataShard) GetStorageFile(chunkIdx uint64) *DataFile {
	for _, df := range ds.dataFiles {
		if df.Contains(chunkIdx) {
			return df
		}
	}
	return nil
}

func (ds *DataShard) Read(meta pora.PhyAddr, isMasked bool) ([]byte, error) {
	if !ds.Contains(meta.KvIdx) {
		return nil, fmt.Errorf("kv not found")
	}
	if meta.KvSize > int(ds.kvSize) {
		return nil, fmt.Errorf("read len too large")
	}
	var (
		data []byte
		err  error
	)
	readLen := meta.KvSize
	for i := uint64(0); i < ds.chunksPerKv; i++ {
		if readLen == 0 {
			break
		}

		chunkReadLen := readLen
		if chunkReadLen > int(CHUNK_SIZE) {
			chunkReadLen = int(CHUNK_SIZE)
		}
		readLen = readLen - chunkReadLen

		chunkIdx := meta.KvIdx*ds.chunksPerKv + i
		var cdata []byte
		if isMasked {
			cdata, err = ds.ReadChunk(chunkIdx, chunkReadLen, common.Hash{}, true)
		} else {
			chunkHash := pora.CalcChunkHash(meta.Commit, chunkIdx, ds.dataFiles[0].miner)
			cdata, err = ds.ReadChunk(chunkIdx, chunkReadLen, chunkHash, false)
		}

		if err != nil {
			return nil, err
		}
		data = append(data, cdata...)
	}
	return data, nil
}

func (ds *DataShard) Write(meta pora.PhyAddr, b []byte, isMasked bool) error {
	if !ds.Contains(meta.KvIdx) {
		return fmt.Errorf("kv not found")
	}

	if uint64(len(b)) > ds.kvSize {
		return fmt.Errorf("write data too large")
	}

	var err error
	for i := uint64(0); i < ds.chunksPerKv; i++ {
		off := int(i * CHUNK_SIZE)
		if off >= len(b) {
			break
		}
		writeLen := len(b) - off
		if writeLen > int(CHUNK_SIZE) {
			writeLen = int(CHUNK_SIZE)
		}

		chunkIdx := meta.KvIdx*ds.chunksPerKv + i
		if isMasked {
			err = ds.WriteChunk(chunkIdx, b[off:off+writeLen], common.Hash{}, true)
		} else {
			chunkHash := pora.CalcChunkHash(meta.Commit, chunkIdx, ds.dataFiles[0].miner)
			err = ds.WriteChunk(chunkIdx, b[off:off+writeLen], chunkHash, false)
		}

		if err != nil {
			return nil
		}
	}
	return nil
}

func (ds *DataShard) ReadChunk(chunkIdx uint64, readLen int, chunkHash common.Hash, isMasked bool) ([]byte, error) {
	for _, df := range ds.dataFiles {
		if df.Contains(chunkIdx) {
			return df.Read(chunkIdx, readLen, chunkHash, isMasked)
		}
	}
	return nil, fmt.Errorf("chunk not found: the shard is not completed?")
}

func (ds *DataShard) WriteChunk(chunkIdx uint64, b []byte, chunkHash common.Hash, isMasked bool) error {
	for _, df := range ds.dataFiles {
		if df.Contains(chunkIdx) {
			return df.Write(chunkIdx, b, chunkHash, isMasked)
		}
	}
	return fmt.Errorf("chunk not found: the shard is not completed?")
}
