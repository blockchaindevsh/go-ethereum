package common

import (
	"math/rand"
	"time"
)

func MakeTestCase(group int, preNums int) (map[int][]int, map[int]int) {

	preTxInGroup := make(map[int]int, 0)

	rand.Seed(time.Now().Unix())
	groupLen := rand.Intn(group)
	if groupLen == 0 {
		groupLen = group
	}

	txLen := 0
	args := make([]int, 0)
	for index := 0; index < groupLen; index++ {
		t := rand.Intn(preNums)
		if t == 0 {
			t = preNums
		}
		args = append(args, t)
		txLen += t
	}

	ans := make(map[int][]int, 0)

	for index := 0; index < txLen; index++ {
		for true {
			randomGroup := rand.Intn(groupLen)
			if len(ans[randomGroup]) == args[randomGroup] {
				continue
			} else {
				if _, ok := ans[randomGroup]; !ok {
					ans[randomGroup] = make([]int, 0)
				}
				ans[randomGroup] = append(ans[randomGroup], index)

				if len(ans[randomGroup]) != 1 {
					preTxInGroup[index] = ans[randomGroup][len(ans[randomGroup])-2]
				} else {
					preTxInGroup[index] = -1
				}

				break
			}
		}
	}
	return ans, preTxInGroup
}
