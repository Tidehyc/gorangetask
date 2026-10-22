//go:build !windows

package main

import (
	"os"
	"strconv"
	"strings"
)

// 这一组函数只负责"把跑测机器的身份记进 JSON"。取不到就返回 unknown / 0，
// 不用默认值假装拿到了——基线文档要能回答"在哪台机器上测的"，猜出来的值比没有更糟。

// cpuModel 从 /proc/cpuinfo 取 CPU 型号。
func cpuModel() string {
	return firstLineValue("/proc/cpuinfo", "model name")
}

// memTotalKB 从 /proc/meminfo 取物理内存总量（KiB）。
func memTotalKB() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				n, _ := strconv.ParseInt(fields[1], 10, 64)
				return n
			}
		}
	}
	return 0
}

// hostLoad 返回 1/5/15 分钟 load average。非 Linux 的 unix 上没有 /proc，返回 unknown。
func hostLoad() string {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "unknown"
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return "unknown"
	}
	return strings.Join(fields[:3], " ")
}

// cpuUtil 是 Windows 专用字段（那里没有 load average）；非 Windows 返回空串，
// 结果 JSON 里对应的键会被 omitempty 去掉。
func cpuUtil() string { return "" }

func firstLineValue(path, key string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, key) {
			if _, v, ok := strings.Cut(line, ":"); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	return "unknown"
}
