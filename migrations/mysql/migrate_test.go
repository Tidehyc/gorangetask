package mysqlmigrations

import (
	"errors"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "single statement without trailing semicolon",
			in:   "CREATE TABLE t (id INT)",
			want: []string{"CREATE TABLE t (id INT)"},
		},
		{
			name: "single statement with trailing semicolon",
			in:   "CREATE TABLE t (id INT);",
			want: []string{"CREATE TABLE t (id INT)"},
		},
		{
			name: "multiple statements",
			in:   "CREATE TABLE a (id INT);\nCREATE INDEX i ON a (id);",
			want: []string{"CREATE TABLE a (id INT)", "CREATE INDEX i ON a (id)"},
		},
		{
			name: "blank statements are dropped",
			in:   ";;\nCREATE TABLE a (id INT);;\n\n",
			want: []string{"CREATE TABLE a (id INT)"},
		},
		{
			name: "line comments are removed",
			in:   "-- header comment\nCREATE TABLE a (id INT); -- trailing\n",
			want: []string{"CREATE TABLE a (id INT)"},
		},
		{
			name: "hash comments are removed",
			in:   "# header\nSELECT 1;",
			want: []string{"SELECT 1"},
		},
		{
			name: "block comments are removed",
			in:   "/* multi\nline */\nSELECT 1;",
			want: []string{"SELECT 1"},
		},
		{
			name: "empty input yields nothing",
			in:   "  \n-- only a comment\n",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("splitStatements() = %q, want %q", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("splitStatements()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestStripComments(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"line comment", "a -- gone\nb", "a \nb"},
		{"hash comment", "a # gone\nb", "a \nb"},
		{"block comment", "a /* gone */ b", "a  b"},
		{"unterminated block comment truncates", "a /* never closed", "a "},
		{"no comments", "SELECT 1", "SELECT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripComments(tt.in); got != tt.want {
				t.Fatalf("stripComments(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestLoadMigrationsParsesAndSorts(t *testing.T) {
	fsys := fstest.MapFS{
		"0010_add_index.sql":    {Data: []byte("CREATE INDEX i ON t (c);")},
		"0002_create_table.sql": {Data: []byte("CREATE TABLE t (id INT);")},
		"0001_init.sql":         {Data: []byte("SELECT 1;")},
		"notes.txt":             {Data: []byte("ignored")},
		"sub/0003_ignored.sql":  {Data: []byte("SELECT 1;")},
	}

	got, err := loadMigrations(fsys)
	if err != nil {
		t.Fatalf("loadMigrations() unexpected error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("loadMigrations() returned %d migrations, want 3", len(got))
	}

	wantVersions := []int64{1, 2, 10}
	wantNames := []string{"init", "create_table", "add_index"}
	for i := range got {
		if got[i].version != wantVersions[i] {
			t.Errorf("migration[%d].version = %d, want %d", i, got[i].version, wantVersions[i])
		}
		if got[i].name != wantNames[i] {
			t.Errorf("migration[%d].name = %q, want %q", i, got[i].name, wantNames[i])
		}
	}

	// 版本 10 排在 2 之后，证明是按数字排序而非文件名字典序。
	if got[1].version != 2 || got[2].version != 10 {
		t.Errorf("migrations are not sorted numerically: %d, %d", got[1].version, got[2].version)
	}
}

func TestLoadMigrationsErrors(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
	}{
		{
			name: "no migration files",
			fsys: fstest.MapFS{"README.md": {Data: []byte("x")}},
		},
		{
			name: "bad filename",
			fsys: fstest.MapFS{"create_table.sql": {Data: []byte("SELECT 1;")}},
		},
		{
			name: "zero version",
			fsys: fstest.MapFS{"0000_init.sql": {Data: []byte("SELECT 1;")}},
		},
		{
			name: "duplicate version",
			fsys: fstest.MapFS{
				"0001_a.sql": {Data: []byte("SELECT 1;")},
				"0001_b.sql": {Data: []byte("SELECT 1;")},
			},
		},
		{
			name: "empty file",
			fsys: fstest.MapFS{"0001_init.sql": {Data: []byte("-- only a comment\n")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadMigrations(tt.fsys)
			if err == nil {
				t.Fatal("loadMigrations() = nil, want error")
			}
			if !errors.Is(err, ErrInvalidMigrationFile) {
				t.Fatalf("loadMigrations() error = %v, want to wrap ErrInvalidMigrationFile", err)
			}
		})
	}
}

func TestEmbeddedMigrationsAreValid(t *testing.T) {
	got, err := loadMigrations(sqlFS)
	if err != nil {
		t.Fatalf("embedded migrations failed to load: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("no embedded migrations found")
	}

	// 0001 必须存在且创建任务表。
	var found int64
	for _, m := range got {
		if m.version == 1 {
			found = m.version
			joined := strings.Join(m.stmts, ";")
			if !strings.Contains(joined, "CREATE TABLE gotask_tasks") {
				t.Errorf("migration 1 does not create gotask_tasks: %s", joined)
			}
		}
	}
	if found != 1 {
		t.Error("migration version 1 not found in embedded migrations")
	}
}
