//go:build windows

package main

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// Windows 上没有 /proc、也没有 load average，所以这里是另一套采集：
// CPU 型号读注册表，内存问 GlobalMemoryStatusEx，负载改用"1 秒采样的 CPU 利用率"。
// 语义与 Unix 的 load average 不同（没有排队长度、没有 5/15 分钟平滑），
// 因此 JSON 里单列 cpu_util_* 字段，而不是塞进 load_avg_* 里冒充同一个量。

func cpuModel() string {
	var key syscall.Handle
	err := syscall.RegOpenKeyEx(
		syscall.HKEY_LOCAL_MACHINE,
		syscall.StringToUTF16Ptr(`HARDWARE\DESCRIPTION\System\CentralProcessor\0`),
		0, syscall.KEY_READ, &key)
	if err != nil {
		if v := os.Getenv("PROCESSOR_IDENTIFIER"); v != "" {
			return v
		}
		return "unknown"
	}
	defer syscall.RegCloseKey(key)

	var buf [512]byte
	size := uint32(len(buf))
	if err := syscall.RegQueryValueEx(key, syscall.StringToUTF16Ptr("ProcessorNameString"),
		nil, nil, &buf[0], &size); err != nil {
		return "unknown"
	}
	// buf 是按字节收到的 UTF-16，按 uint16 重新切片再转字符串。
	u16 := unsafe.Slice((*uint16)(unsafe.Pointer(&buf[0])), size/2)
	for i, c := range u16 {
		if c == 0 {
			u16 = u16[:i]
			break
		}
	}
	if s := syscall.UTF16ToString(u16); s != "" {
		return s
	}
	return "unknown"
}

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// memTotalKB 返回物理内存总量（KiB）；调用失败返回 0。
func memTotalKB() int64 {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("GlobalMemoryStatusEx")
	var m memoryStatusEx
	m.Length = uint32(unsafe.Sizeof(m))
	r, _, _ := proc.Call(uintptr(unsafe.Pointer(&m)))
	if r == 0 {
		return 0
	}
	return int64(m.TotalPhys / 1024)
}

// hostLoad 在 Windows 上没有对应概念，明确写 n/a 而不是编一个数。
func hostLoad() string { return "n/a" }

// cpuUtil 采样 1 秒的系统 CPU 利用率。GetSystemTimes 的 kernel 时间包含 idle，
// 所以总时间 = kernel + user，忙 = 总时间 − idle。
func cpuUtil() string {
	idle1, kernel1, user1, ok := systemTimes()
	if !ok {
		return "unknown"
	}
	time.Sleep(time.Second)
	idle2, kernel2, user2, ok := systemTimes()
	if !ok {
		return "unknown"
	}
	total := (kernel2 - kernel1) + (user2 - user1)
	if total == 0 {
		return "unknown"
	}
	busy := total - (idle2 - idle1)
	return fmt.Sprintf("%.1f%%", 100*float64(busy)/float64(total))
}

func systemTimes() (idle, kernel, user uint64, ok bool) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("GetSystemTimes")
	var i, k, u syscall.Filetime
	r, _, _ := proc.Call(
		uintptr(unsafe.Pointer(&i)),
		uintptr(unsafe.Pointer(&k)),
		uintptr(unsafe.Pointer(&u)))
	if r == 0 {
		return 0, 0, 0, false
	}
	ft := func(t syscall.Filetime) uint64 {
		return uint64(t.HighDateTime)<<32 | uint64(t.LowDateTime)
	}
	return ft(i), ft(k), ft(u), true
}
