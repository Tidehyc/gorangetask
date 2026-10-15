package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// RenewLease 的入参校验不连库（sql.Open 只解析 DSN）。
//
// 凭据只能来自 Claim 的返回值：ID 或 owner 为空就是调用方写错了，不该发一条
// 注定 0 行的 UPDATE 去数据库上确认。租约时长非正数更危险——TIMESTAMPADD 加 0
// 会让 lease_until 停在当前时刻，等于"续租成功但租约立刻过期"，比报错难查得多。
func TestRenewLeaseRejectsInvalidInput(t *testing.T) {
	c := newTestClient(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	valid := gorangetask.ClaimedTask{ID: 1, Owner: "owner", Epoch: 1}

	tests := []struct {
		name    string
		task    gorangetask.ClaimedTask
		lease   time.Duration
		wantErr error
	}{
		{"zero task id", gorangetask.ClaimedTask{Owner: "owner", Epoch: 1}, time.Second, gorangetask.ErrInvalidTask},
		{"empty owner", gorangetask.ClaimedTask{ID: 1, Epoch: 1}, time.Second, gorangetask.ErrInvalidTask},
		{"zero lease duration", valid, 0, gorangetask.ErrInvalidConfig},
		{"negative lease duration", valid, -time.Second, gorangetask.ErrInvalidConfig},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.RenewLease(ctx, tt.task, tt.lease)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("RenewLease() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// ReapExpiredLeases 的入参校验同样不连库。
//
// 非正的 limit 必须被挡住：它会变成 SQL 里的 `LIMIT 0` 或负数，前者让回收器永远
// 什么也不做（而它恰恰是崩溃遗留任务的唯一出口），后者直接是语法错误。这两种都
// 属于配置写错，应该在本地就报出来，而不是变成一个安静失效的保障。
func TestReapExpiredLeasesRejectsInvalidLimit(t *testing.T) {
	c := newTestClient(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	for _, limit := range []int{0, -1} {
		result, err := c.ReapExpiredLeases(ctx, limit)
		if !errors.Is(err, gorangetask.ErrInvalidConfig) {
			t.Fatalf("ReapExpiredLeases(limit=%d) error = %v, want ErrInvalidConfig", limit, err)
		}
		if result != (gorangetask.ReapResult{}) {
			t.Errorf("ReapExpiredLeases(limit=%d) result = %+v, want zero value", limit, result)
		}
	}
}
