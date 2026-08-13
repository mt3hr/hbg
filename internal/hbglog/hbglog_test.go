package hbglog

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readRecords はログファイルを 1 行 1 レコードとして読み込みます。
func readRecords(t *testing.T, path string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ログを読み込めない %s: %v", path, err)
	}

	records := []map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("JSONとして読めない行がある %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

func TestInitWritesSplitAndMergedFiles(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelInfo, Version: "test"})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer l.Close()

	slog.Info("情報のメッセージ")
	slog.Warn("警告のメッセージ")
	slog.Error("エラーのメッセージ")
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// レベル別ファイルに、そのレベルのものだけが入ること
	info := readRecords(t, filepath.Join(dir, "hbg_info.log"))
	if len(info) != 1 || info[0]["msg"] != "情報のメッセージ" {
		t.Errorf("hbg_info.log の内容が想定と違う: %v", info)
	}
	warn := readRecords(t, filepath.Join(dir, "hbg_warn.log"))
	if len(warn) != 1 || warn[0]["msg"] != "警告のメッセージ" {
		t.Errorf("hbg_warn.log の内容が想定と違う: %v", warn)
	}

	// 統合ファイルには全部入ること。
	// gkill では SplitOnly 固定のため統合ファイルが常に空だった。
	merged := readRecords(t, filepath.Join(dir, "hbg.log"))
	if len(merged) != 3 {
		t.Errorf("hbg.log のレコード数 = %d, want 3", len(merged))
	}
}

// gkill の実装は WithAttrs で属性を捨てており、共通フィールドが
// 出力に現れませんでした。ここでは付与されることを確認します。
func TestStaticFieldsArePresent(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelInfo, Version: "1.2.3"})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	slog.Info("hello")
	l.Close()

	records := readRecords(t, filepath.Join(dir, "hbg_info.log"))
	if len(records) != 1 {
		t.Fatalf("レコード数 = %d", len(records))
	}
	if records[0]["app"] != "hbg" {
		t.Errorf("app フィールドがない: %v", records[0])
	}
	if records[0]["version"] != "1.2.3" {
		t.Errorf("version フィールドがない: %v", records[0])
	}
}

func TestWithAttrsKeepsAttributes(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelInfo})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	slog.Default().With("run_id", "abc123").Info("hello")
	l.Close()

	records := readRecords(t, filepath.Join(dir, "hbg_info.log"))
	if len(records) != 1 {
		t.Fatalf("レコード数 = %d", len(records))
	}
	if records[0]["run_id"] != "abc123" {
		t.Errorf("With で足した属性が失われている: %v", records[0])
	}
}

func TestMinLevelFiltersOutput(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelWarn})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	slog.Info("出ないはず")
	slog.Warn("出るはず")
	l.Close()

	// 下位レベルのファイルはそもそも作られない
	if _, err := os.Stat(filepath.Join(dir, "hbg_info.log")); !os.IsNotExist(err) {
		t.Error("下限より低いレベルのファイルが作られている")
	}
	warn := readRecords(t, filepath.Join(dir, "hbg_warn.log"))
	if len(warn) != 1 {
		t.Errorf("警告のレコード数 = %d, want 1", len(warn))
	}
}

func TestLevelNoneWritesNothing(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelNone})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	slog.Error("これは出ない")
	l.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("none 指定なのにファイルが作られている: %v", entries)
	}
}

func TestTransferLogRecordsOnePerFile(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelTransfer})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	LogTransfer(TransferRecord{
		SrcStorage: "local", SrcPath: "/a.txt",
		DstStorage: "dropbox", DstPath: "/backup",
		Bytes: 1024, Duration: 500 * time.Millisecond, Result: ResultCopied,
	})
	LogTransfer(TransferRecord{
		SrcStorage: "local", SrcPath: "/b.txt",
		DstStorage: "dropbox", DstPath: "/backup",
		Result: ResultFailed, Err: os.ErrPermission,
	})
	l.Close()

	records := readRecords(t, filepath.Join(dir, "hbg_transfer.log"))
	if len(records) != 2 {
		t.Fatalf("レコード数 = %d, want 2", len(records))
	}

	if records[0]["result"] != ResultCopied {
		t.Errorf("1件目の result = %v", records[0]["result"])
	}
	if records[0]["src_path"] != "/a.txt" {
		t.Errorf("1件目の src_path = %v", records[0]["src_path"])
	}
	if records[0]["bytes_per_sec"] == nil {
		t.Error("転送速度が記録されていない")
	}
	if records[1]["result"] != ResultFailed {
		t.Errorf("2件目の result = %v", records[1]["result"])
	}
	if records[1]["error"] == nil {
		t.Error("失敗の理由が記録されていない")
	}
}

// 進捗表示の制御文字がログに混ざらないことを確認します。
func TestLogHasNoControlCharacters(t *testing.T) {
	dir := t.TempDir()

	l, err := Init(Options{Dir: dir, MinLevel: LevelInfo})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	slog.Info("メッセージ")
	l.Close()

	data, err := os.ReadFile(filepath.Join(dir, "hbg_info.log"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.ContainsRune(string(data), '\x1b') {
		t.Error("ログにエスケープシーケンスが含まれている")
	}
}

func TestParseLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"none":     LevelNone,
		"error":    LevelError,
		"warn":     LevelWarn,
		"INFO":     LevelInfo,
		"transfer": LevelTransfer,
		"debug":    LevelDebug,
		" trace ":  LevelTrace,
	}
	for name, want := range tests {
		got, err := ParseLevel(name)
		if err != nil {
			t.Errorf("ParseLevel(%q): %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", name, got, want)
		}
	}

	if _, err := ParseLevel("そんなレベルはない"); err == nil {
		t.Error("不明なレベルがエラーにならない")
	}
}
