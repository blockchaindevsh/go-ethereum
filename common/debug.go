// Copyright 2015 The go-ethereum Authors
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
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// Report gives off a warning requesting the user to submit an issue to the github tracker.
func Report(extra ...interface{}) {
	fmt.Fprintln(os.Stderr, "You've encountered a sought after, hard to reproduce bug. Please report this to the developers <3 https://github.com/ethereum/go-ethereum/issues")
	fmt.Fprintln(os.Stderr, extra...)

	_, file, line, _ := runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "%v:%v\n", file, line)

	debug.PrintStack()

	fmt.Fprintln(os.Stderr, "#### BUG! PLEASE REPORT ####")
}

// PrintDepricationWarning prinst the given string in a box using fmt.Println.
func PrintDepricationWarning(str string) {
	line := strings.Repeat("#", len(str)+4)
	emptyLine := strings.Repeat(" ", len(str))
	fmt.Printf(`
%s
# %s #
# %s #
# %s #
%s

`, line, emptyLine, str, emptyLine, line)
}

type DebugTime struct {
	ExecuteTx     time.Duration
	ValidateBlock time.Duration
	WriteBlock    time.Duration
	CommitTrie    time.Duration
	TxLen         int

	jList []jiankongList
}

type jiankongList struct {
	cpu        float64
	useMem     float64
	totalMem   float64
	usePercent float64
	read       float64
	write      float64
}

func NewDebugTime() *DebugTime {
	d := &DebugTime{
		TxLen:         0,
		ExecuteTx:     time.Duration(0),
		ValidateBlock: time.Duration(0),
		WriteBlock:    time.Duration(0),
		CommitTrie:    time.Duration(0),
		jList:         make([]jiankongList, 0),
	}
	go d.jiankong()
	return d

}

func (d *DebugTime) jiankong() {
	for true {
		cpu := GetCpuPercent()
		useMem, totalMem, usedPercent := GetMemPercent()
		read, write := GetDiskIO()
		//fmt.Printf("cpu=%.2f usedMem=%.2f totalMem=%.2f usedPercent=%.2f read=%vMB write=%vMB\n", cpu, useMem, totalMem, usedPercent, read, write)
		d.jList = append(d.jList, jiankongList{
			cpu:        cpu,
			useMem:     useMem,
			totalMem:   totalMem,
			usePercent: usedPercent,
			read:       read,
			write:      write,
		})

		time.Sleep(2 * 60 * time.Second)
	}
}

func GetCpuPercent() float64 {
	percent, _ := cpu.Percent(time.Second, false)
	return percent[0]
}

func GetMemPercent() (float64, float64, float64) {
	memInfo, _ := mem.VirtualMemory()
	used := float64(memInfo.Used) / 1024 / 1024 / 1024
	total := float64(memInfo.Total) / 1024 / 1024 / 1024
	return used, total, memInfo.UsedPercent
}

func GetDiskIO() (float64, float64) {
	tag := "D:"
	d1, _ := disk.IOCounters("D:")
	time.Sleep(1 * time.Second)
	d2, _ := disk.IOCounters("D:")
	return float64(d2[tag].ReadBytes-d1[tag].ReadBytes) / 1024 / 1024, float64(d2[tag].WriteBytes-d1[tag].WriteBytes) / 1024 / 1024

}
func (d *DebugTime) Print() {
	fmt.Println("执行区块数目", 200000)
	fmt.Println("总的交易数目", d.TxLen)

	fmt.Println("执行区块用时", d.ExecuteTx)
	fmt.Println("验证区块用时", d.ValidateBlock)
	fmt.Println("写入区块用时", d.WriteBlock)
	fmt.Println("写入trie用时", d.CommitTrie)

	fmt.Println(" cpu,usedMem,totalMem,usePercent,read,write")
	for _, v := range d.jList {
		fmt.Printf("%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n", v.cpu, v.useMem, v.totalMem, v.usePercent, v.read, v.write)
	}
}

var (
	DebugInfo         = NewDebugTime()
	BlockExecuteBatch = int(1)
)
