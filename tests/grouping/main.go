package main

import "fmt"

type addr int

type tx struct {
	from addr
	to   addr
}

var (
	rootAddr = make(map[addr]addr, 0)
)

func Find(x addr) addr {
	if rootAddr[x] != x {
		rootAddr[x] = Find(rootAddr[x])
	}
	return rootAddr[x]
}

func Union(x addr, y addr) {
	if _, ok := rootAddr[x]; !ok {
		rootAddr[x] = x
	}

	if _, ok := rootAddr[y]; !ok {
		rootAddr[y] = y
	}
	fx := Find(x)
	fy := Find(y)
	if fx != fy {
		rootAddr[fy] = fx
	}
}

func grouping(txs []*tx) {
	rootAddr = make(map[addr]addr, 0)
	for _, tx := range txs {
		Union(tx.from, tx.to)
	}
	for index := 1; index <= 5; index++ {
		fmt.Printf("id=%v 根节点=%v\n", index, Find(addr(index)))
	}
	groupList := make(map[addr][]int, 0)
	groupID := make(map[addr]int, 0)

	for index, tx := range txs {
		rootAddr := Find(tx.from)
		id, exist := groupID[rootAddr]
		if !exist {
			id = len(groupList)
			groupID[rootAddr] = id

		}
		groupList[addr(id)] = append(groupList[addr(id)], index)
	}
	fmt.Println("组的个数", len(groupList))
	for index := 0; index < len(groupList); index++ {
		list := ""
		for _, v := range groupList[addr(index)] {
			list += fmt.Sprintf("%v ", v)
		}
		fmt.Printf("组id=%v 交易序号=%v\n", index, list)
	}
	fmt.Printf("\n\n")

}

func main() {
	/*
		case1:
			输入:
			    1-2
		   		2-4
				3-5
		   		2-1
		case2:
			输入:
				1-2
				2-4
				3-5
				2-3
	*/

	case1 := []*tx{
		&tx{
			from: 1,
			to:   2,
		},
		&tx{
			from: 2,
			to:   4,
		},
		&tx{
			from: 3,
			to:   5,
		},
		&tx{
			from: 2,
			to:   1,
		},
	}
	case2 := []*tx{
		&tx{
			from: 1,
			to:   2,
		},
		&tx{
			from: 2,
			to:   4,
		},
		&tx{
			from: 3,
			to:   5,
		},
		&tx{
			from: 2,
			to:   3,
		},
	}
	grouping(case1)
	grouping(case2)
}
