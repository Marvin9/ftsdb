package ftsdb

import (
	"math"
	"strconv"
)

type ArrayOps interface {
	Insert(element interface{})
	At(index int) interface{}
}

type DataBlock struct {
	elements []interface{}
	capacity int
	size     int
}

func newDataBlock(capacity int) DataBlock {
	return DataBlock{
		elements: make([]interface{}, capacity),
		capacity: capacity,
		size:     0,
	}
}

func (db *DataBlock) IsFull() bool {
	return db.size >= db.capacity
}

func (db *DataBlock) Insert(element interface{}) {
	db.elements[db.size] = element
	db.size++
}

type FastArray struct {
	index                         []DataBlock
	currentSuperBlock             int
	dataBlocksInCurrentSuperBlock int
	totalDataBlocks               int
	size                          int
}

func NewFastArray() *FastArray {
	return &FastArray{
		index:                         make([]DataBlock, 0),
		currentSuperBlock:             0,
		totalDataBlocks:               0,
		dataBlocksInCurrentSuperBlock: 0,
	}
}

func fastPow2(value int) int64 {
	return 1 << value
}

func superBlockCapacity(superblock int) int {
	return int(fastPow2(superblock))
}

func (arr *FastArray) currentSuperBlockCapacity() int {
	return superBlockCapacity(arr.currentSuperBlock)
}

func superBlockDataBlockCapacity(superblock int) int {
	return int(fastPow2(superblock / 2))
}

func (arr *FastArray) currentSuperBlockDataBlockCapacity() int {
	return superBlockDataBlockCapacity(arr.currentSuperBlock)
}

func dataBlockCapacity(superblock int) int {
	return int(fastPow2(int(math.Ceil(float64(superblock) / 2))))
}

func (arr *FastArray) currentDataBlockCapacity() int {
	return dataBlockCapacity(arr.currentSuperBlock)
}

func (arr *FastArray) allocateNewDataBlock() {
	if arr.dataBlocksInCurrentSuperBlock >= arr.currentSuperBlockDataBlockCapacity() {
		arr.dataBlocksInCurrentSuperBlock = 0
		arr.currentSuperBlock++
	}

	arr.index = append(arr.index, newDataBlock(arr.currentDataBlockCapacity()))
	arr.dataBlocksInCurrentSuperBlock++
	arr.totalDataBlocks++
}

func (arr *FastArray) getLastDataBlock() *DataBlock {
	return &arr.index[arr.totalDataBlocks-1]
}

func (arr *FastArray) Insert(element interface{}) {
	if arr.size == 0 {
		arr.allocateNewDataBlock()
	}

	lastDataBlock := arr.getLastDataBlock()

	if lastDataBlock.IsFull() {
		arr.allocateNewDataBlock()
	}

	lastDataBlock = arr.getLastDataBlock()

	lastDataBlock.Insert(element)

	arr.size++
}

func (arr *FastArray) At(index int) interface{} {
	r := strconv.FormatInt(int64(index+1), 2)

	superblockIndex := len(r) - 1

	firstDataBlockOfSuperBlockIndex := -1

	if superblockIndex%2 == 0 {
		firstDataBlockOfSuperBlockIndex = 2 * int(fastPow2(superblockIndex/2)-1)
	} else {
		firstDataBlockOfSuperBlockIndex = (3 * int(fastPow2((superblockIndex-1)/2))) - 2
	}

	firstIndexOfSuperBlock := math.Pow(2, float64(superblockIndex)) - 1

	offset := index - int(firstIndexOfSuperBlock)

	currDataBlockCapacity := dataBlockCapacity(superblockIndex)

	finalDataBlock := firstDataBlockOfSuperBlockIndex + (offset / currDataBlockCapacity)

	offsetInsideDatablock := offset % currDataBlockCapacity

	return arr.index[finalDataBlock].elements[offsetInsideDatablock]
}
