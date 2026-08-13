package transfer_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/transfer"
)

// newPair はテスト用の転送元と転送先を返します。
func newPair(t *testing.T) (*memory.Storage, *memory.Storage) {
	t.Helper()
	return memory.New("src"), memory.New("dst")
}

func put(t *testing.T, s *memory.Storage, path, content string) {
	t.Helper()
	_, err := s.Put(context.Background(), path, strings.NewReader(content), storage.ObjectMeta{
		Size:    int64(len(content)),
		ModTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("Put(%s): %v", path, err)
	}
}

func baseOptions(src, dst storage.Storage) transfer.Options {
	return transfer.Options{
		Src:     src,
		Dst:     dst,
		SrcPath: "/data",
		DstDir:  "/backup",
		Workers: 2,
		Compare: transfer.DefaultComparePolicy(),
		Retry:   transfer.RetryPolicy{MaxAttempts: 1},
	}
}

func TestRunCopiesTree(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/sub/b.txt", "bbb")
	put(t, src, "/data/sub/deep/c.txt", "ccc")

	result, err := transfer.Run(context.Background(), baseOptions(src, dst))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.Transferred != 3 || result.Failed != 0 {
		t.Errorf("Transferred=%d Failed=%d, want 3 と 0", result.Transferred, result.Failed)
	}

	snap := dst.Snapshot()
	for path, want := range map[string]string{
		"/backup/data/a.txt":          "aaa",
		"/backup/data/sub/b.txt":      "bbb",
		"/backup/data/sub/deep/c.txt": "ccc",
	} {
		if snap[path] != want {
			t.Errorf("%s = %q, want %q", path, snap[path], want)
		}
	}
}

func TestRunSkipsIdenticalFiles(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")

	opts := baseOptions(src, dst)
	first, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("1回目: %v", err)
	}
	if first.Transferred != 1 {
		t.Fatalf("1回目 Transferred=%d, want 1", first.Transferred)
	}

	second, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("2回目: %v", err)
	}
	if second.Transferred != 0 || second.Skipped != 1 {
		t.Errorf("2回目 Transferred=%d Skipped=%d, want 0 と 1", second.Transferred, second.Skipped)
	}
}

// 空のディレクトリも転送先に作られることを確認します。
func TestRunCreatesEmptyDirectories(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	if err := src.Mkdir(context.Background(), "/data/emptydir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	if _, err := transfer.Run(context.Background(), baseOptions(src, dst)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	info, err := dst.Stat(context.Background(), "/backup/data/emptydir")
	if err != nil {
		t.Fatalf("空のディレクトリが転送先にない: %v", err)
	}
	if !info.IsDir {
		t.Error("ディレクトリとして作られていない")
	}
}

func TestRunIgnoresNames(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/Thumbs.db", "x")

	opts := baseOptions(src, dst)
	opts.Ignore = []string{"Thumbs.db"}

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 1 {
		t.Errorf("Transferred=%d, want 1", result.Transferred)
	}
	if _, ok := dst.Snapshot()["/backup/data/Thumbs.db"]; ok {
		t.Error("無視するはずのファイルが転送されている")
	}
}

// 転送元が存在しない場合はエラーになることを確認します。
//
// 以前は一致するものがなくても「0件成功」で正常終了しており、
// パスを打ち間違えてもスクリプトからは成功に見えていました。
func TestRunFailsWhenSourceMissing(t *testing.T) {
	src, dst := newPair(t)

	opts := baseOptions(src, dst)
	opts.SrcPath = "/そんなものはない"

	if _, err := transfer.Run(context.Background(), opts); err == nil {
		t.Fatal("転送元が存在しないのに成功した")
	}
}

func TestRunSingleFile(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/only.txt", "内容")

	opts := baseOptions(src, dst)
	opts.SrcPath = "/data/only.txt"

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 1 {
		t.Errorf("Transferred=%d, want 1", result.Transferred)
	}
	if got := dst.Snapshot()["/backup/only.txt"]; got != "内容" {
		t.Errorf("/backup/only.txt = %q", got)
	}
}

func TestRunDryRunChangesNothing(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/sub/b.txt", "bbb")

	opts := baseOptions(src, dst)
	opts.DryRun = true

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 2 {
		t.Errorf("Transferred=%d, want 2（転送する予定の件数）", result.Transferred)
	}
	if snap := dst.Snapshot(); len(snap) != 0 {
		t.Errorf("dry-run なのに転送先が変化している: %v", snap)
	}
}

// 個々のファイルの失敗が集計され、全体は止まらないことを確認します。
func TestRunCollectsFailures(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/ok1.txt", "1")
	put(t, src, "/data/bad.txt", "2")
	put(t, src, "/data/ok2.txt", "3")

	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "put" && strings.HasSuffix(path, "bad.txt") {
				return errors.New("書き込みに失敗しました")
			}
			return nil
		},
	})

	result, err := transfer.Run(context.Background(), baseOptions(src, dst))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.Transferred != 2 {
		t.Errorf("Transferred=%d, want 2", result.Transferred)
	}
	if result.Failed != 1 {
		t.Errorf("Failed=%d, want 1", result.Failed)
	}
	if len(result.Errors) != 1 {
		t.Errorf("Errors=%d件, want 1", len(result.Errors))
	}
	// 失敗しなかったものは転送されていること
	if len(dst.Snapshot()) != 2 {
		t.Errorf("転送先の件数 = %d, want 2", len(dst.Snapshot()))
	}
}

func TestRunMaxErrorsAborts(t *testing.T) {
	src, dst := newPair(t)
	for i := range 20 {
		put(t, src, fmt.Sprintf("/data/f%02d.txt", i), "x")
	}

	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, _ string) error {
			if op == "put" {
				return errors.New("常に失敗します")
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.MaxErrors = 3

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !result.Aborted {
		t.Error("上限に達したのに中断していない")
	}
	if result.Failed > 5 {
		t.Errorf("Failed=%d, 上限の付近で止まるべき", result.Failed)
	}
}

func TestRunCancellation(t *testing.T) {
	src, dst := newPair(t)
	for i := range 50 {
		put(t, src, fmt.Sprintf("/data/f%02d.txt", i), strings.Repeat("x", 1000))
	}

	ctx, cancel := context.WithCancel(context.Background())

	var seen atomic.Int32
	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, _ string) error {
			if op == "put" && seen.Add(1) == 5 {
				cancel()
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Workers = 1

	result, err := transfer.Run(ctx, opts)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	// 取り消しは失敗として数えない
	if result != nil && result.Failed != 0 {
		t.Errorf("Failed=%d, 取り消しは失敗に数えない", result.Failed)
	}
}
