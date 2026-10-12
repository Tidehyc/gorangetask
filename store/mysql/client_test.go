package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// newTestClient 构造一个不连库的 Client。
// sql.Open 只解析 DSN、不建立连接，所以这些测试不依赖 MySQL。
func newTestClient(t *testing.T, cfg gorangetask.ClientConfig) *Client {
	t.Helper()

	db, err := sql.Open("mysql", "root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	c, err := New(db, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestNewRejectsInvalidInput(t *testing.T) {
	valid := func() *sql.DB {
		db, err := sql.Open("mysql", "root:rootpw@tcp(127.0.0.1:3306)/gotask")
		if err != nil {
			t.Fatalf("sql.Open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	t.Run("nil db", func(t *testing.T) {
		_, err := New(nil, gorangetask.ClientConfig{})
		if !errors.Is(err, gorangetask.ErrInvalidConfig) {
			t.Fatalf("want ErrInvalidConfig, got %v", err)
		}
	})

	t.Run("client config is validated", func(t *testing.T) {
		_, err := New(valid(), gorangetask.ClientConfig{MaxPayloadSize: -1})
		if !errors.Is(err, gorangetask.ErrInvalidConfig) {
			t.Fatalf("want ErrInvalidConfig, got %v", err)
		}
	})

	t.Run("default queue respects mysql column width", func(t *testing.T) {
		// 65 个字符，超过 VARCHAR(64)。
		_, err := New(valid(), gorangetask.ClientConfig{DefaultQueue: strings.Repeat("q", maxQueueNameLen+1)})
		if !errors.Is(err, gorangetask.ErrInvalidConfig) {
			t.Fatalf("want ErrInvalidConfig, got %v", err)
		}
	})

	t.Run("default queue at column width is accepted", func(t *testing.T) {
		if _, err := New(valid(), gorangetask.ClientConfig{DefaultQueue: strings.Repeat("队", maxQueueNameLen)}); err != nil {
			t.Fatalf("64 个多字节字符应当合法，得到 %v", err)
		}
	})
}

func TestNewAppliesDefaults(t *testing.T) {
	c := newTestClient(t, gorangetask.ClientConfig{})

	if c.cfg.DefaultQueue != gorangetask.DefaultQueue {
		t.Errorf("DefaultQueue = %q, want %q", c.cfg.DefaultQueue, gorangetask.DefaultQueue)
	}
	if c.cfg.DefaultMaxAttempts != gorangetask.DefaultMaxAttempts {
		t.Errorf("DefaultMaxAttempts = %d, want %d", c.cfg.DefaultMaxAttempts, gorangetask.DefaultMaxAttempts)
	}
	if c.cfg.MaxPayloadSize != gorangetask.DefaultMaxPayloadSize {
		t.Errorf("MaxPayloadSize = %d, want %d", c.cfg.MaxPayloadSize, gorangetask.DefaultMaxPayloadSize)
	}
}

// TestPrepare 覆盖校验与归一化的全部分支。prepare 是 Enqueue / EnqueueTx 共用的
// 前置逻辑，这里测透之后，两个入口的差别只剩"在谁的连接上执行"。
func TestPrepare(t *testing.T) {
	shanghai := time.FixedZone("CST", 8*3600)

	// 默认配置的 Client：默认队列 default、默认次数 10、payload 上限 256 KiB。
	def := newTestClient(t, gorangetask.ClientConfig{})

	// 自定义配置的 Client，用来验证"配置真的生效"。
	custom := newTestClient(t, gorangetask.ClientConfig{
		DefaultQueue:       "reports",
		DefaultMaxAttempts: 3,
		MaxPayloadSize:     16,
	})

	validJSON := []byte(`{"order_id":42}`)

	tests := []struct {
		name    string
		client  *Client
		task    gorangetask.Task
		wantErr error

		// 期望的归一化结果，仅在 wantErr == nil 时校验。
		wantQueue       string
		wantType        string
		wantMaxAttempts int
		wantRunAt       any
		wantIdemKey     any
	}{
		{
			name:            "minimal task falls back to configured defaults",
			client:          def,
			task:            gorangetask.Task{Type: "send_email", Payload: validJSON},
			wantQueue:       gorangetask.DefaultQueue,
			wantType:        "send_email",
			wantMaxAttempts: gorangetask.DefaultMaxAttempts,
			wantRunAt:       nil,
			wantIdemKey:     nil,
		},
		{
			name:   "custom defaults are used",
			client: custom,
			task:   gorangetask.Task{Type: "send_email", Payload: validJSON},

			wantQueue:       "reports",
			wantType:        "send_email",
			wantMaxAttempts: 3,
			wantRunAt:       nil,
			wantIdemKey:     nil,
		},
		{
			name:   "explicit fields are preserved",
			client: def,
			task: gorangetask.Task{
				Queue:          "critical",
				Type:           "charge_card",
				Payload:        validJSON,
				MaxAttempts:    5,
				IdempotencyKey: "order-42",
			},
			wantQueue:       "critical",
			wantType:        "charge_card",
			wantMaxAttempts: 5,
			wantRunAt:       nil,
			wantIdemKey:     "order-42",
		},
		{
			name:   "run_at is normalized to a utc string",
			client: def,
			task: gorangetask.Task{
				Type:    "send_email",
				Payload: validJSON,
				RunAt:   time.Date(2026, 10, 12, 20, 30, 45, 123456000, shanghai),
			},
			wantQueue:       gorangetask.DefaultQueue,
			wantType:        "send_email",
			wantMaxAttempts: gorangetask.DefaultMaxAttempts,
			wantRunAt:       "2026-10-12 12:30:45.123456",
		},
		{
			name:   "run_at nanoseconds are truncated to microseconds",
			client: def,
			task: gorangetask.Task{
				Type:    "send_email",
				Payload: validJSON,
				RunAt:   time.Date(2026, 10, 12, 12, 0, 0, 123456789, time.UTC),
			},
			wantQueue:       gorangetask.DefaultQueue,
			wantType:        "send_email",
			wantMaxAttempts: gorangetask.DefaultMaxAttempts,
			wantRunAt:       "2026-10-12 12:00:00.123456",
		},
		{
			name:            "json null is a valid payload",
			client:          def,
			task:            gorangetask.Task{Type: "ping", Payload: []byte("null")},
			wantQueue:       gorangetask.DefaultQueue,
			wantType:        "ping",
			wantMaxAttempts: gorangetask.DefaultMaxAttempts,
		},
		{
			name:            "payload exactly at the limit is accepted",
			client:          custom,
			task:            gorangetask.Task{Type: "ping", Payload: []byte(`"0123456789ab"`)},
			wantQueue:       "reports",
			wantType:        "ping",
			wantMaxAttempts: 3,
		},
		{
			name:            "queue name at column width is accepted",
			client:          def,
			task:            gorangetask.Task{Queue: strings.Repeat("队", maxQueueNameLen), Type: "ping", Payload: validJSON},
			wantQueue:       strings.Repeat("队", maxQueueNameLen),
			wantType:        "ping",
			wantMaxAttempts: gorangetask.DefaultMaxAttempts,
		},
		{
			name:            "idempotency key at column width is accepted",
			client:          def,
			task:            gorangetask.Task{Type: "ping", Payload: validJSON, IdempotencyKey: strings.Repeat("k", maxIdempotencyKeyLen)},
			wantQueue:       gorangetask.DefaultQueue,
			wantType:        "ping",
			wantMaxAttempts: gorangetask.DefaultMaxAttempts,
			wantIdemKey:     strings.Repeat("k", maxIdempotencyKeyLen),
		},

		// —— 失败分支 ——
		{
			name:    "empty task type is rejected",
			client:  def,
			task:    gorangetask.Task{Payload: validJSON},
			wantErr: gorangetask.ErrInvalidTask,
		},
		{
			name:    "task type beyond column width is rejected",
			client:  def,
			task:    gorangetask.Task{Type: strings.Repeat("t", maxTaskTypeLen+1), Payload: validJSON},
			wantErr: gorangetask.ErrInvalidTask,
		},
		{
			name:    "queue name beyond column width is rejected",
			client:  def,
			task:    gorangetask.Task{Queue: strings.Repeat("q", maxQueueNameLen+1), Type: "ping", Payload: validJSON},
			wantErr: gorangetask.ErrInvalidTask,
		},
		{
			name:    "negative max attempts is rejected",
			client:  def,
			task:    gorangetask.Task{Type: "ping", Payload: validJSON, MaxAttempts: -1},
			wantErr: gorangetask.ErrInvalidTask,
		},
		{
			name:    "idempotency key beyond column width is rejected",
			client:  def,
			task:    gorangetask.Task{Type: "ping", Payload: validJSON, IdempotencyKey: strings.Repeat("k", maxIdempotencyKeyLen+1)},
			wantErr: gorangetask.ErrInvalidTask,
		},
		{
			name:    "empty payload is rejected",
			client:  def,
			task:    gorangetask.Task{Type: "ping"},
			wantErr: gorangetask.ErrInvalidPayload,
		},
		{
			name:    "invalid json payload is rejected",
			client:  def,
			task:    gorangetask.Task{Type: "ping", Payload: []byte(`{"order_id":`)},
			wantErr: gorangetask.ErrInvalidPayload,
		},
		{
			name:    "payload beyond the limit is rejected",
			client:  def,
			task:    gorangetask.Task{Type: "ping", Payload: []byte(`"` + strings.Repeat("x", gorangetask.DefaultMaxPayloadSize) + `"`)},
			wantErr: gorangetask.ErrInvalidPayload,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := tc.client.prepare(tc.task)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if p.queue != tc.wantQueue {
				t.Errorf("queue = %q, want %q", p.queue, tc.wantQueue)
			}
			if p.taskType != tc.wantType {
				t.Errorf("taskType = %q, want %q", p.taskType, tc.wantType)
			}
			if p.maxAttempts != tc.wantMaxAttempts {
				t.Errorf("maxAttempts = %d, want %d", p.maxAttempts, tc.wantMaxAttempts)
			}
			if p.runAt != tc.wantRunAt {
				t.Errorf("runAt = %v, want %v", p.runAt, tc.wantRunAt)
			}
			if p.idempotencyKey != tc.wantIdemKey {
				t.Errorf("idempotencyKey = %v, want %v", p.idempotencyKey, tc.wantIdemKey)
			}
			if string(p.payload) != string(tc.task.Payload) {
				t.Errorf("payload = %q, want %q (input payload must pass through unchanged)",
					p.payload, tc.task.Payload)
			}
		})
	}
}

// TestPrepareIdempotencyKeyEmptyBecomesNull 单独固化"空幂等键必须落成 NULL"这条：
// 若写成空串，同一队列的第二条无幂等键任务会被唯一索引误判为重复。
func TestPrepareIdempotencyKeyEmptyBecomesNull(t *testing.T) {
	c := newTestClient(t, gorangetask.ClientConfig{})

	p, err := c.prepare(gorangetask.Task{Type: "ping", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if p.idempotencyKey != nil {
		t.Fatalf("empty idempotency key must bind as NULL, got %#v", p.idempotencyKey)
	}
}

func TestEnqueueTxRejectsNilTransaction(t *testing.T) {
	c := newTestClient(t, gorangetask.ClientConfig{})

	_, err := c.EnqueueTx(context.Background(), nil, gorangetask.Task{Type: "ping", Payload: []byte(`{}`)})
	if !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Fatalf("want ErrInvalidTask, got %v", err)
	}
}

func TestIsDuplicateEntry(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"mysql 1062", &gomysql.MySQLError{Number: mysqlErrDuplicateEntry}, true},
		{"wrapped mysql 1062", fmt.Errorf("insert task: %w", &gomysql.MySQLError{Number: mysqlErrDuplicateEntry}), true},
		{"mysql 1064", &gomysql.MySQLError{Number: 1064}, false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDuplicateEntry(tc.err); got != tc.want {
				t.Fatalf("isDuplicateEntry(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
