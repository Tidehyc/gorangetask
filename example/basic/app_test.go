// 本文件的用例**不需要数据库**：它们测的是装配期的入参校验与失败路径。
// 因此它们不会跟着集成测试一起被 skip——`example/basic` 里唯一需要 MySQL 的
// 是 example_integration_test.go（那个文件在缺 DSN 时整体跳过）。
package basic_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Tidehyc/gorangetask/example/basic"
)

// TestOpenRejectsEmptyDSN 断言空连接串被挡在最前面：库不替调用方猜"连哪个库"。
func TestOpenRejectsEmptyDSN(t *testing.T) {
	app, err := basic.Open(context.Background(), "", basic.Options{})
	if err == nil {
		_ = app.Close()
		t.Fatal("Open accepted an empty DSN")
	}
	if !strings.Contains(err.Error(), "DSN must not be empty") {
		t.Errorf("Open error = %v, want it to say the DSN must not be empty", err)
	}
}

// TestOpenRejectsMalformedDSN 断言写法不对的连接串不会被当成"能用"。
func TestOpenRejectsMalformedDSN(t *testing.T) {
	// 缺 tcp(...) 主机部分：驱动在打开连接时就会拒绝，而不是等到第一次查询。
	app, err := basic.Open(context.Background(), "not-a-mysql-dsn", basic.Options{})
	if err == nil {
		_ = app.Close()
		t.Fatal("Open accepted a malformed DSN")
	}
}

// TestOpenFailsWhenDatabaseIsUnreachable 断言 Ping 失败时 Open 立刻返回错误，
// 而不是把"连不上库"拖到第一次使用才炸——那会让故障点离原因很远。
func TestOpenFailsWhenDatabaseIsUnreachable(t *testing.T) {
	// 127.0.0.1:1 上不会有 MySQL。DSN 里的 timeout 给个上限，免得环境异常时挂住；
	// 正常情况下这里是立刻的 connection refused。
	dsn := "root:rootpw@tcp(127.0.0.1:1)/gotask?parseTime=true&loc=UTC&timeout=1s"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := basic.Open(ctx, dsn, basic.Options{})
	if err == nil {
		_ = app.Close()
		t.Fatal("Open succeeded against an unreachable server")
	}
	if !strings.Contains(err.Error(), "ping database") {
		t.Errorf("Open error = %v, want it to mention the failed ping", err)
	}
}
