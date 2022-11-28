package sstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/sstorage/pora"
)

const (

	// keccak256(b'Web3Q Large Storage')[0:8]
	MAGIC   = uint64(0xcf20bd770c22b2e1)
	VERSION = uint64(1)

	CHUNK_SIZE = uint64(4096)
)

type DataFile struct {
	file          *os.File
	chunkIdxStart uint64
	chunkIdxLen   uint64
	maxKvSize     uint64
	miner         common.Address
}

type DataFileHeader struct {
	magic         uint64
	version       uint64
	chunkIdxStart uint64
	chunkIdxLen   uint64
	maxKvSize     uint64
	miner         common.Address
	status        uint64
}

func calcChunkHash(commit [24]byte, chunkIdx uint64, addr common.Address) common.Hash {
	return common.Hash{}
}

type PhyAddr struct {
	KvIdx  uint64
	KvSize int
	Commit [24]byte
}

func UnmaskKvData(epoch uint64, phyAddr PhyAddr, addr common.Address, maxKvSize uint64, maskedData, unmaskedData []byte) (bool, []byte) {
	if len(unmaskedData) != len(maskedData) {
		unmaskedData = make([]byte, len(maskedData))
	}
	if phyAddr.KvSize != len(maskedData) {
		return false, nil
	}

	maskBuffer := make([]byte, CHUNK_SIZE)
	startChunkIdx := maxKvSize / CHUNK_SIZE * phyAddr.KvIdx
	for i := 0; i < phyAddr.KvSize/int(CHUNK_SIZE); i++ {
		chunkHash := calcChunkHash(phyAddr.Commit, startChunkIdx+uint64(i), addr)
		getMaskDataWInChunk(epoch, chunkHash, maxKvSize, int(CHUNK_SIZE), maskBuffer)
		for j := 0; j < int(CHUNK_SIZE); j++ {
			unmaskedData[i*int(CHUNK_SIZE)+j] = maskedData[i*int(CHUNK_SIZE)+j] ^ maskBuffer[j]
		}
	}

	tailBytes := phyAddr.KvSize % int(CHUNK_SIZE)
	if tailBytes > 0 {
		i := phyAddr.KvSize / int(CHUNK_SIZE)
		chunkHash := calcChunkHash(phyAddr.Commit, startChunkIdx+uint64(i), addr)
		getMaskDataWInChunk(epoch, chunkHash, maxKvSize, tailBytes, maskBuffer[0:tailBytes])
		for j := 0; j < tailBytes; j++ {
			unmaskedData[i*int(CHUNK_SIZE)+j] = maskedData[i*int(CHUNK_SIZE)+j] ^ maskBuffer[j]
		}
	}

	// TODO verify unmaskedData against phyAddr

	return true, unmaskedData

}

func getKvMaskData(epoch uint64, phyAddr PhyAddr, addr common.Address, maxKvSize uint64, maskBuffer []byte) []byte {
	if len(maskBuffer) != phyAddr.KvSize {
		maskBuffer = make([]byte, phyAddr.KvSize)
	}

	startChunkIdx := maxKvSize / CHUNK_SIZE * phyAddr.KvIdx
	for i := 0; i < phyAddr.KvSize/int(CHUNK_SIZE); i++ {
		chunkHash := calcChunkHash(phyAddr.Commit, startChunkIdx+uint64(i), addr)
		getMaskDataWInChunk(epoch, chunkHash, maxKvSize, int(CHUNK_SIZE), maskBuffer[i*int(CHUNK_SIZE):(i+1)*int(CHUNK_SIZE)])
	}

	tailBytes := phyAddr.KvSize % int(CHUNK_SIZE)
	if tailBytes > 0 {
		i := phyAddr.KvSize / int(CHUNK_SIZE)
		chunkHash := calcChunkHash(phyAddr.Commit, startChunkIdx+uint64(i), addr)
		getMaskDataWInChunk(epoch, chunkHash, maxKvSize, tailBytes, maskBuffer[i*int(CHUNK_SIZE):])
	}

	return maskBuffer
}

func getMaskDataWInChunk(epoch uint64, chunkHash common.Hash, maxKvSize uint64, sizeInChunk int, maskBuffer []byte) []byte {

	if sizeInChunk > int(CHUNK_SIZE) {
		panic("sizeInChunk > CHUNK_SIZE")
	}
	if len(maskBuffer) != sizeInChunk {
		maskBuffer = make([]byte, sizeInChunk)
	}

	cache := pora.Cache(epoch)
	size := ethash.DatasetSizeForEpoch(epoch)

	realHash := make([]byte, len(chunkHash)+8)
	copy(realHash, chunkHash[:])

	for i := 0; i < sizeInChunk/ethash.GetMixBytes(); i++ {
		pora.ToRealHash(chunkHash, maxKvSize, uint64(i), realHash, false)
		mask := ethash.HashimotoForMaskLight(size, cache.Cache, realHash)
		if len(mask) != ethash.GetMixBytes() {
			panic("#mask != MixBytes")
		}
		copy(maskBuffer[i*ethash.GetMixBytes():], mask)
	}

	tailBytes := sizeInChunk % ethash.GetMixBytes()
	if tailBytes > 0 {
		i := sizeInChunk / ethash.GetMixBytes()
		pora.ToRealHash(chunkHash, maxKvSize, uint64(i), realHash, false)
		mask := ethash.HashimotoForMaskLight(size, cache.Cache, realHash)
		if len(mask) != ethash.GetMixBytes() {
			panic("#mask != MixBytes")
		}
		copy(maskBuffer[i*ethash.GetMixBytes():], mask)
	}

	return maskBuffer
}

// Mask the data in place
func MaskDataInPlace(maskData []byte, userData []byte) []byte {
	if len(userData) > len(maskData) {
		panic("user data can not be larger than mask data")
	}
	for i := 0; i < len(userData); i++ {
		maskData[i] = maskData[i] ^ userData[i]
	}
	return maskData
}

// Unmask the data in place
func UnmaskDataInPlace(userData []byte, maskData []byte) []byte {
	if len(userData) > len(maskData) {
		panic("user data can not be larger than mask data")
	}
	for i := 0; i < len(userData); i++ {
		userData[i] = maskData[i] ^ userData[i]
	}
	return userData
}

func Create(filename string, chunkIdxStart uint64, chunkIdxLen uint64, epoch, maxKvSize uint64, miner common.Address) (*DataFile, error) {
	log.Info("Creating file", "filename", filename)
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < chunkIdxLen; i++ {
		chunkIdx := chunkIdxStart + i
		chunkHash := calcChunkHash([24]byte{}, chunkIdx, miner)
		_, err := file.WriteAt(getMaskDataWInChunk(epoch, chunkHash, maxKvSize, int(CHUNK_SIZE), nil), int64((chunkIdx+1)*CHUNK_SIZE))
		if err != nil {
			return nil, err
		}
	}
	dataFile := &DataFile{
		file:          file,
		chunkIdxStart: chunkIdxStart,
		chunkIdxLen:   chunkIdxLen,
		maxKvSize:     maxKvSize,
		miner:         miner,
	}
	dataFile.writeHeader()
	return dataFile, nil
}

func OpenDataFile(filename string) (*DataFile, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	dataFile := &DataFile{
		file: file,
	}
	return dataFile, dataFile.readHeader()
}

func (df *DataFile) Contains(chunkIdx uint64) bool {
	return chunkIdx >= df.chunkIdxStart && chunkIdx < df.ChunkIdxEnd()
}

func (df *DataFile) ChunkIdxEnd() uint64 {
	return df.chunkIdxStart + df.chunkIdxLen
}

func (df *DataFile) Read(chunkIdx uint64, len int, hash common.Hash, isMasked bool) ([]byte, error) {
	if !df.Contains(chunkIdx) {
		return nil, fmt.Errorf("chunk not found")
	}
	md := make([]byte, len)
	n, err := df.file.ReadAt(md, int64(chunkIdx+1)*int64(CHUNK_SIZE))
	if err != nil {
		return nil, err
	}
	if n != len {
		return nil, fmt.Errorf("not full read")
	}
	if isMasked {
		return md, nil
	} else {
		return UnmaskDataInPlace(md, getMaskData(chunkIdx, df.maskType)), nil
	}
}

func (df *DataFile) Write(chunkIdx uint64, b []byte, isMasked bool) error {
	if !df.Contains(chunkIdx) {
		return fmt.Errorf("chunk not found")
	}

	if len(b) > int(CHUNK_SIZE) {
		return fmt.Errorf("write data too large")
	}

	if isMasked {
		b = MaskDataInPlace(getMaskData(chunkIdx, df.maskType), b)
	} else {

	}
	_, err := df.file.WriteAt(b, int64(chunkIdx+1)*int64(CHUNK_SIZE))
	return err
}

func (df *DataFile) writeHeader() error {
	header := DataFileHeader{
		magic:         MAGIC,
		version:       VERSION,
		chunkIdxStart: df.chunkIdxStart,
		chunkIdxLen:   df.chunkIdxLen,
		maxKvSize:     df.maxKvSize,
		miner:         df.miner,
		status:        0,
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, header.magic); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, header.version); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, header.chunkIdxStart); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, header.chunkIdxLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, header.maxKvSize); err != nil {
		return err
	}
	n, err := buf.Write(header.miner[:])
	if err != nil {
		return err
	}
	if n != len(header.miner) {
		return fmt.Errorf("short write for header.miner, n=%d", n)
	}
	if err := binary.Write(buf, binary.BigEndian, header.status); err != nil {
		return err
	}
	if _, err := df.file.WriteAt(buf.Bytes(), 0); err != nil {
		return err
	}
	return nil
}

func (df *DataFile) readHeader() error {
	header := DataFileHeader{
		magic:   MAGIC,
		version: VERSION,
		status:  0,
	}

	b := make([]byte, CHUNK_SIZE)
	n, err := df.file.ReadAt(b, 0)
	if err != nil {
		return err
	}
	if n != int(CHUNK_SIZE) {
		return fmt.Errorf("not full header read")
	}

	buf := bytes.NewBuffer(b)
	if err := binary.Read(buf, binary.BigEndian, &header.magic); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &header.version); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &header.chunkIdxStart); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &header.chunkIdxLen); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &header.maxKvSize); err != nil {
		return err
	}
	n, err = buf.Read(header.miner[:])
	if err != nil {
		return err
	}
	if n != len(header.miner) {
		return fmt.Errorf("short read for header.miner, n=%d", n)
	}
	if err := binary.Read(buf, binary.BigEndian, &header.status); err != nil {
		return err
	}

	// Sanity check
	if header.magic != MAGIC {
		return fmt.Errorf("magic error")
	}
	if header.version > VERSION {
		return fmt.Errorf("unsupported version")
	}

	df.chunkIdxStart = header.chunkIdxStart
	df.chunkIdxLen = header.chunkIdxLen
	df.maxKvSize = header.maxKvSize
	df.miner = header.miner

	return nil
}
