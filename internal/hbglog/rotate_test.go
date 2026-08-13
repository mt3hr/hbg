package hbglog

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRotatingFileRotatesBySize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	// 100バイトで世代交代、3世代保持
	rf, err := newRotatingFile(path, 100, 3)
	if err != nil {
		t.Fatalf("newRotatingFile: %v", err)
	}
	defer rf.Close()

	line := strings.Repeat("x", 60) + "\n" // 61バイト
	for i := 0; i < 5; i++ {
		if _, err := rf.Write([]byte(line)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	rf.Close()

	// 現在のファイルは上限を超えていないこと
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() > 100 {
		t.Errorf("現在のファイルが上限を超えている: %d バイト", info.Size())
	}

	// 世代が作られていること
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("1世代目が作られていない: %v", err)
	}
}

func TestRotatingFileKeepsMaxBackups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	rf, err := newRotatingFile(path, 50, 2) // 2世代まで
	if err != nil {
		t.Fatalf("newRotatingFile: %v", err)
	}

	line := strings.Repeat("y", 40) + "\n"
	for i := 0; i < 10; i++ {
		if _, err := rf.Write([]byte(line)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	rf.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	// 現在のファイル + 2世代 = 3個まで
	if len(entries) > 3 {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("保持世代数を超えている（%d個）: %v", len(entries), names)
	}
	// 保持数を超える世代は存在しないこと
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Error("保持数を超えた世代が残っている")
	}
}

func TestRotatingFileAppendsToExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	if err := os.WriteFile(path, []byte("既存の内容\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	rf, err := newRotatingFile(path, 1<<20, 3)
	if err != nil {
		t.Fatalf("newRotatingFile: %v", err)
	}
	if _, err := rf.Write([]byte("追記\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	rf.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "既存の内容") {
		t.Error("既存の内容が失われている")
	}
	if !strings.Contains(string(data), "追記") {
		t.Error("追記されていない")
	}
}

// 実運用に近い形で、詳細なログを大量に出しても
// ファイルが際限なく肥大化しないことを確認します。
// gkill ではローテーションがないため、この状況で無制限に増えます。
func TestLogsDoNotGrowUnbounded(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{
		Dir:          dir,
		MinLevel:     LevelTrace,
		MaxSizeBytes: 4096,
		MaxBackups:   2,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	for i := 0; i < 2000; i++ {
		slog.Debug("詳細なログ", "index", i, "padding", strings.Repeat("z", 100))
	}
	l.Close()

	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}

	// 統合ファイルと debug ファイルそれぞれが (現在 + 2世代) に収まる。
	// 上限に多少の余裕を見ても、無制限でないことは十分に示せる。
	const limit = 4096 * 3 * 2 * 2
	if total > limit {
		t.Errorf("ログの合計が %d バイトで上限 %d を超えている", total, limit)
	}
	if total == 0 {
		t.Error("ログがまったく書かれていない")
	}
}

func TestRemoveOldLogs(t *testing.T) {
	dir := t.TempDir()

	oldFile := filepath.Join(dir, "hbg_info.log.3")
	newFile := filepath.Join(dir, "hbg_info.log")
	for _, p := range []string{oldFile, newFile} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldFile, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	removeOldLogs(dir, 24*time.Hour)

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("古いログが削除されていない")
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Errorf("新しいログまで削除されている: %v", err)
	}
}

func TestRemoveOldLogsIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()

	unrelated := filepath.Join(dir, "important.txt")
	if err := os.WriteFile(unrelated, []byte("大事なファイル"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(unrelated, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	removeOldLogs(dir, 24*time.Hour)

	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("関係ないファイルが削除された: %v", err)
	}
}

func BenchmarkLogging(b *testing.B) {
	dir := b.TempDir()
	l, err := Init(Options{Dir: dir, MinLevel: LevelInfo})
	if err != nil {
		b.Fatalf("Init: %v", err)
	}
	defer l.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slog.Info("メッセージ", "index", i, "path", fmt.Sprintf("/data/%d.txt", i))
	}
}
