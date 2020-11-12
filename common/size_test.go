// Copyright 2014 The go-ethereum Authors
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

package common

import (
	"container/heap"
	"fmt"
	"testing"
	"time"
)

func TestStorageSizeString(t *testing.T) {
	tests := []struct {
		size StorageSize
		str  string
	}{
		{2381273, "2.27 MiB"},
		{2192, "2.14 KiB"},
		{12, "12.00 B"},
	}

	for _, test := range tests {
		if test.size.String() != test.str {
			t.Errorf("%f: got %q, want %q", float64(test.size), test.size.String(), test.str)
		}
	}
}

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type SortTxManager struct {
	heap *IntHeap
}

func NewSortTxManager(from, to []Address) *SortTxManager {
	groupList := make(map[int][]int, 0) //并查集
	groupList[1] = []int{1, 8, 10}
	groupList[2] = []int{2, 7, 11}
	groupList[3] = []int{3, 6, 12}
	groupList[4] = []int{4, 5, 9}

	dependMp := make(map[int]int)
	for _, list := range groupList {
		for index := 0; index < len(list)-1; index++ {
			dependMp[list[index]] = list[index+1]
		}
	}

	firstList := make(IntHeap, 0)
	for _, v := range groupList {
		firstList = append(firstList, v[0])
	}
	heap.Init(&firstList)

	return &SortTxManager{heap: &firstList}
}

func (s *SortTxManager) AddTx() {

}

func (s *SortTxManager) GetTx() {

}

func TestABC(t *testing.T) {
	mp := make(map[int][]int, 0)
	mp[1] = []int{1, 8, 10}
	mp[2] = []int{2, 7, 11}
	mp[3] = []int{3, 6, 12}
	mp[4] = []int{4, 5, 9}

	dependMp := make(map[int]int)
	for _, list := range mp {

		index := 0
		for ; index < len(list)-1; index++ {
			dependMp[list[index]] = list[index+1]
		}
	}
	fmt.Println("depend", dependMp)

	firstList := make(IntHeap, 0)
	//for _, v := range mp {
	//	firstList = append(firstList, v[0])
	//}
	firstList = []int{1, 3, 2, 4}

	heap.Init(&firstList)
	for true {
		len := firstList.Len()
		if len == 0 {
			break
		}
		data := heap.Pop(&firstList)
		nextValue := dependMp[data.(int)]
		if nextValue != 0 {
			heap.Push(&firstList, nextValue)
		}

		fmt.Println("len", len, data, firstList)
		time.Sleep(1 * time.Second)
	}

}
