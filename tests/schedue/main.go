package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type intHeap []int

func (h intHeap) Len() int           { return len(h) }
func (h intHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h intHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *intHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *intHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type txSortManager struct {
	mu   sync.Mutex
	heap *intHeap

	groupLen           int
	nextTxIndexInGroup map[int]int
}

func NewSortTxManager(groupList map[int][]int) *txSortManager {
	nextTxIndexInGroup := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			nextTxIndexInGroup[list[index]] = list[index+1]
		}
	}

	heapList := make(intHeap, 0)
	for _, v := range groupList {
		heapList = append(heapList, v[0])
	}
	heap.Init(&heapList)

	return &txSortManager{
		heap:               &heapList,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
	}
}

func (s *txSortManager) pushNextTxInGroup(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		heap.Push(s.heap, nextTxIndex)
	}
}

func (s *txSortManager) push(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	heap.Push(s.heap, txIndex)
}

func (s *txSortManager) pop() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return -1
	}
	return heap.Pop(s.heap).(int)
}

type schedule struct {
	mp map[int][]int
	mu sync.Mutex

	sortManager   *txSortManager
	txChannel     chan int
	mergedChannel chan struct{}

	txLen       int
	results     []*result
	mergedIndex int
	endSignel   chan struct{}
	end         bool
}

type result struct {
	txIndex int
}

func newManager(mp map[int][]int) *schedule {
	txLen := 0
	for _, txs := range mp {
		txLen += len(txs)
	}
	s := &schedule{
		mp:            mp,
		txChannel:     make(chan int, 0),
		sortManager:   NewSortTxManager(mp),
		mergedChannel: make(chan struct{}, txLen),

		txLen:       txLen,
		results:     make([]*result, txLen, txLen),
		mergedIndex: -1,
		endSignel:   make(chan struct{}, 0),
	}
	thread := 2
	for index := 0; index < thread; index++ {
		i := index
		go s.txLoop(i)
	}
	go s.scheduleLoop()
	return s
}

func (s *schedule) txLoop(loopIndex int) {
	defer fmt.Printf("txLoop=%v end\n", loopIndex)
	for true {
		txIndex, ok := <-s.txChannel
		if !ok {
			break
		}

		s.mu.Lock()
		mergedIndex := s.mergedIndex
		s.mu.Unlock()
		fmt.Printf("timeStamp=%v txLoopID=%v 基于%v执行%v 开始\n", time.Now().Nanosecond(), loopIndex, mergedIndex, txIndex)
		s.handleTx(txIndex)
		fmt.Printf("timeStamp=%v txLoopID=%v 基于%v执行%v 完成\n", time.Now().Nanosecond(), loopIndex, mergedIndex, txIndex)
	}
}

func (s *schedule) scheduleLoop() {
	defer fmt.Printf("schedule end\n")
	for true {
		for true {
			data := s.sortManager.pop()
			if data == -1 {
				break
			}
			s.txChannel <- data
		}
		_, ok := <-s.mergedChannel
		if !ok {
			break
		}
	}
}

func (s *schedule) handleResult(result2 *result) {
	s.results[result2.txIndex] = result2
	start := result2.txIndex
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.end {
		return
	}

	for s.mergedIndex+1 == start && start < s.txLen && s.results[start] != nil {
		s.mergedIndex++
		fmt.Printf("timeStamp=%v 合并完毕 当前mergedIndex=%v\n", time.Now().Nanosecond(), s.mergedIndex)
		s.sortManager.pushNextTxInGroup(start)

		start++
		s.mergedChannel <- struct{}{}
	}
	if s.mergedIndex+1 == s.txLen && !s.end {
		s.end = true
		close(s.mergedChannel)
		close(s.txChannel)
		s.endSignel <- struct{}{}
	}
}

func (s *schedule) handleTx(txIndex int) {
	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
	go s.handleResult(&result{txIndex: txIndex})
}

func main() {
	/*
		调度：
			case1:
				组id   组内交易序号
		 		  0       0,1,3
		          1       2,5
		          2       4,6

			heap
				0   1,3
				2   2,5
				4    6

	*/

	s := newManager(map[int][]int{
		0: []int{0, 1, 3},
		1: []int{2, 5},
		2: []int{4, 6},
	})
	<-s.endSignel
	fmt.Println("执行结束")
	time.Sleep(2 * time.Second)
}
