package main

import "testing"

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
