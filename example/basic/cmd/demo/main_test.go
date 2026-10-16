package main

import (
	"testing"
	"time"
)

// TestStopReasonNeverStopsWithNothingTracked 钉住 `-orders 0` 的行为：那种模式下本次
// 没有创建任何订单，"全都完成了"是句空话——不特判的话第一次 tick 就会退出，完全看不到
// Worker 接手遗留任务的过程。这正是踩过的坑（`-orders 0` 起来 3 秒就自己收工了）。
//
// allDone 传 true 是有意的：它就是那次踩坑时的取值。
func TestStopReasonNeverStopsWithNothingTracked(t *testing.T) {
	reason, stopNow := stopReason(0, true)
	if stopNow {
		t.Fatalf("stopReason(0, true) = (%q, true); want no stop: with -orders 0 the demo must "+
			"keep running until Ctrl-C", reason)
	}
	if reason != "" {
		t.Errorf("stopReason(0, true) reason = %q, want empty", reason)
	}
}

// TestStopReasonStopsWhenEveryTrackedOrderIsDone 钉住另一种模式：本次创建的订单全部导出
// 完成后应当自己收工，不必等 Ctrl-C。
func TestStopReasonStopsWhenEveryTrackedOrderIsDone(t *testing.T) {
	reason, stopNow := stopReason(3, true)
	if !stopNow {
		t.Fatal("stopReason(3, true) did not request a stop; want the demo to finish on its own")
	}
	if reason != "all orders exported" {
		t.Errorf("stopReason(3, true) reason = %q, want %q", reason, "all orders exported")
	}
}

// TestStopReasonKeepsRunningWhileOrdersRemain 钉住"还有没完成的就继续看"。
func TestStopReasonKeepsRunningWhileOrdersRemain(t *testing.T) {
	reason, stopNow := stopReason(3, false)
	if stopNow {
		t.Fatalf("stopReason(3, false) = (%q, true); want no stop while orders are still pending", reason)
	}
}

// TestWorkerConfigDerivesHeartbeatFromLease 钉住"租约被调小时心跳跟着按 1/3 推导"。
//
// 这条不是装饰：库要求 HeartbeatInterval < LeaseDuration，而默认心跳写死 10s。
// 若 workerConfig 只覆盖 LeaseDuration、把心跳留给库的默认值，那么 `-lease 5s`
// 会让 NewWorker 当场报"心跳必须小于租约"，故障演示的第一步命令就废了。
// 所以这里除了断言 1/3 这个数，还真的走一遍 WithDefaults().Validate()，
// 把"配置合法"也钉住——两条一起才等于"这条命令真能跑起来"。
func TestWorkerConfigDerivesHeartbeatFromLease(t *testing.T) {
	cfg := workerConfig(5*time.Second, 0)
	if cfg.LeaseDuration != 5*time.Second {
		t.Fatalf("LeaseDuration = %s, want 5s", cfg.LeaseDuration)
	}
	if cfg.HeartbeatInterval != 5*time.Second/3 {
		t.Errorf("HeartbeatInterval = %s, want %s (lease/3)", cfg.HeartbeatInterval, 5*time.Second/3)
	}
	if err := cfg.WithDefaults().Validate(); err != nil {
		t.Fatalf("a 5s lease must yield a valid config, got: %v", err)
	}
}

// TestWorkerConfigLeavesDefaultsAloneWhenUnset 钉住"不传开关就是一个字段都不设"。
//
// 防的是顺手把库的默认值抄进 workerConfig：一旦在这里填死 30s，示例就跑不到库
// 真正的默认值，库日后调整默认值时示例会悄悄偏离而无人察觉。
func TestWorkerConfigLeavesDefaultsAloneWhenUnset(t *testing.T) {
	cfg := workerConfig(0, 0)
	if cfg.LeaseDuration != 0 || cfg.HeartbeatInterval != 0 || cfg.ReapInterval != 0 {
		t.Fatalf("workerConfig(0, 0) = {lease:%s heartbeat:%s reap:%s}, want all zero so the library fills defaults",
			cfg.LeaseDuration, cfg.HeartbeatInterval, cfg.ReapInterval)
	}
}

// TestWorkerConfigPassesReapIntervalThrough 钉住回收器间隔原样透传——包括负值。
//
// 负值在库里表示"关闭回收器"，demo 只是转发，不该擅自把负值改写成默认值：
// 那会让一个明确说"关掉回收器"的开关变成"每 30s 回收一次"。
func TestWorkerConfigPassesReapIntervalThrough(t *testing.T) {
	for _, want := range []time.Duration{2 * time.Second, -1} {
		cfg := workerConfig(0, want)
		if cfg.ReapInterval != want {
			t.Errorf("workerConfig(0, %s).ReapInterval = %s, want %s", want, cfg.ReapInterval, want)
		}
		if cfg.LeaseDuration != 0 || cfg.HeartbeatInterval != 0 {
			t.Errorf("workerConfig(0, %s) touched lease fields: lease=%s heartbeat=%s",
				want, cfg.LeaseDuration, cfg.HeartbeatInterval)
		}
	}
}
