package main

import (
	"awesomeProject7/common"
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

type sortTxInterface interface {
	pushNextTxInGroup(txIndex int)
	pop() int
}

type txNoGroup struct {
	txLen     int
	hasResult []bool

	backend *schedule
}

func NewSortNoGroup(groupList map[int][]int, txLen int) *txNoGroup {
	return &txNoGroup{txLen: txLen, hasResult: make([]bool, txLen, txLen)}
}

func (t *txNoGroup) pushNextTxInGroup(txIndex int) {
	t.backend.results[txIndex] = nil
	t.hasResult[txIndex] = false
}

func (t *txNoGroup) pop() int {
	start := t.backend.mergedIndex
	for index := start + 1; index < t.backend.txLen; index++ {
		if t.backend.results[index] == nil && !t.hasResult[index] {
			t.hasResult[index] = true
			return index
		}
	}
	return -1
}

type txGroup struct {
	mu   sync.Mutex
	heap *intHeap

	groupLen           int
	nextTxIndexInGroup map[int]int
}

func NewSortTxManager(groupList map[int][]int, txLen int) *txGroup {
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

	return &txGroup{
		heap:               &heapList,
		groupLen:           len(groupList),
		nextTxIndexInGroup: nextTxIndexInGroup,
	}
}

func (s *txGroup) pushNextTxInGroup(txIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nextTxIndex := s.nextTxIndexInGroup[txIndex]; nextTxIndex != 0 {
		heap.Push(s.heap, nextTxIndex)
	}
}

func (s *txGroup) pop() int {
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

	sortManager   sortTxInterface
	txChannel     chan int
	mergedChannel chan struct{}

	resultChannel chan struct{}

	txLen       int
	results     []*result
	mergedIndex int
	endSignel   chan struct{}
	end         bool
}

type result struct {
	err     error
	base    int
	txIndex int
}

func newManager(mp map[int][]int, group bool) *schedule {
	txLen := 0
	for _, txs := range mp {
		txLen += len(txs)
	}
	s := &schedule{
		mp:        mp,
		txChannel: make(chan int, 0),

		mergedChannel: make(chan struct{}, txLen),
		resultChannel: make(chan struct{}, txLen),

		txLen:       txLen,
		results:     make([]*result, txLen, txLen),
		mergedIndex: -1,
		endSignel:   make(chan struct{}, 0),
	}
	if group {
		s.sortManager = NewSortTxManager(mp, txLen)
	} else {
		s.sortManager = NewSortNoGroup(mp, txLen)
		s.sortManager.(*txNoGroup).backend = s
	}
	thread := 2
	for index := 0; index < thread; index++ {
		i := index
		go s.txLoop(i)
	}
	go s.mergeLoop()
	go s.scheduleLoop()
	return s
}

func (s *schedule) txLoop(loopIndex int) {
	for true {
		txIndex, ok := <-s.txChannel
		if !ok {
			break
		}

		mergedIndex := s.mergedIndex
		//fmt.Printf("timeStamp=%v txLoopID=%v 基于%v执行%v 开始\n", time.Now().Nanosecond(), loopIndex, mergedIndex, txIndex)
		s.handleTx(txIndex)
		fmt.Printf("timeStamp=%v txLoopID=%v 基于%v执行%v 完成\n", time.Now().Nanosecond(), loopIndex, mergedIndex, txIndex)
	}
}

func (s *schedule) scheduleLoop() {
	defer fmt.Printf("schedule end\n")
	for !s.end {
		data := s.sortManager.pop()
		fmt.Printf("timeStamp=%v 准备丢去执行=%v\n", time.Now().Nanosecond(), data)
		if data != -1 {
			s.txChannel <- data
		} else {
			_, ok := <-s.mergedChannel
			if !ok {
				break
			}
		}

	}
}

func (s *schedule) mergeLoop() {
	for !s.end {
		_, ok := <-s.resultChannel
		if !ok {
			break
		}

		needReHandle := false

		start := s.mergedIndex + 1
		for start < s.txLen && s.results[start] != nil {
			needReHandle = true
			if s.results[start].err != nil {
				fmt.Printf("timeStamp=%v 合并失败： 当前%v err=%v\n", time.Now().Nanosecond(), s.mergedIndex, s.results[start].err)
				s.sortManager.pushNextTxInGroup(start)
				break
			}
			s.mergedIndex++
			fmt.Printf("timeStamp=%v 合并成功---------------------- 当前mergedIndex=%v\n", time.Now().Nanosecond(), s.mergedIndex)
			s.sortManager.pushNextTxInGroup(start)
			start++

		}
		if s.mergedIndex+1 == s.txLen && !s.end {
			s.end = true
			close(s.mergedChannel)
			close(s.txChannel)
			s.endSignel <- struct{}{}
			return
		}
		if needReHandle {
			s.mergedChannel <- struct{}{}
		}

	}
}

func (s *schedule) handleTx(txIndex int) {
	var err error
	if s.mergedIndex < preTxInGroup[txIndex] {
		err = fmt.Errorf("tx=%v 基于%v执行 同组上一个为%v", txIndex, s.mergedIndex, preTxInGroup[txIndex])
		fmt.Printf("timeStamp=%v 执行出错=%v\n", time.Now().Nanosecond(), err)
	}

	r := &result{
		err:     err,
		base:    s.mergedIndex,
		txIndex: txIndex,
	}
	time.Sleep(time.Duration(rand.Intn(5000)) * time.Microsecond)
	s.results[txIndex] = r
	s.resultChannel <- struct{}{}
}

var (
	groupMp, preTxInGroup = common.MakeTestCase(5, 2)
)

func main() {
	fmt.Println("真是依赖情况", groupMp)

	s0 := newManager(groupMp, true)
	<-s0.endSignel
	fmt.Println("执行结束")
	time.Sleep(2 * time.Second)

	fmt.Println("\n\n\n\n")

	s1 := newManager(groupMp, false)
	<-s1.endSignel
	fmt.Println("执行结束")
	time.Sleep(2 * time.Second)

}
