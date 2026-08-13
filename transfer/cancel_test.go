package transfer_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mt3hr/hbg/backend/local"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/transfer"
)

// 転送の途中で中断しても、中身の欠けたファイルや一時ファイルが
// 残らないことを、実際のファイルシステムで確認します。
//
// 以前は転送先へ直接書いていたため、割り込みで中身が欠けたファイルが
// そのまま残り、しかも次回の実行では「サイズが違う」としか分からず
// 壊れていることに気づけませんでした。
func TestCancelLeavesNoPartialFiles(t *testing.T) {
	root := filepath.ToSlash(t.TempDir())
	srcDir := root + "/src"
	dstDir := root + "/dst"

	if err := os.MkdirAll(filepath.FromSlash(srcDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// 途中で止められる程度の大きさのファイルをいくつか用意する
	const fileSize = 2 << 20
	content := strings.Repeat("x", fileSize)
	for i := range 8 {
		path := filepath.FromSlash(fmt.Sprintf("%s/f%d.bin", srcDir, i))
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	s := local.New("local")
	ctx, cancel := context.WithCancel(context.Background())

	// 帯域を絞って、確実に転送の途中で中断できるようにする
	opts := transfer.Options{
		Src:            s,
		Dst:            s,
		SrcPath:        srcDir,
		DstDir:         dstDir,
		Workers:        2,
		Compare:        transfer.DefaultComparePolicy(),
		Retry:          transfer.RetryPolicy{MaxAttempts: 1},
		BandwidthLimit: 4 << 20,
	}

	var transferred atomic.Int32
	opts.OnTransfer = func(ev transfer.TransferEvent) {
		if ev.Err == nil {
			transferred.Add(1)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := transfer.Run(ctx, opts)
		done <- err
	}()

	// いくつか転送が終わったところで中断する
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("中断しても転送が終わらない")
	}

	// 転送先に残っているものは、すべて完全であること
	entries, err := os.ReadDir(filepath.FromSlash(dstDir + "/src"))
	if err != nil {
		if os.IsNotExist(err) {
			return // 1件も転送されないうちに中断した
		}
		t.Fatalf("ReadDir: %v", err)
	}

	for _, entry := range entries {
		if strings.Contains(entry.Name(), "hbgpart") {
			t.Errorf("一時ファイルが残っている: %s", entry.Name())
			continue
		}

		info, err := entry.Info()
		if err != nil {
			t.Errorf("Info: %v", err)
			continue
		}
		if info.Size() != fileSize {
			t.Errorf("%s が %d バイト（中身が欠けている。完全なら %d バイト）",
				entry.Name(), info.Size(), fileSize)
		}
	}
}

// 走査の途中で中断しても、速やかに止まることを確認します。
func TestCancelDuringScan(t *testing.T) {
	root := filepath.ToSlash(t.TempDir())
	srcDir := root + "/src"

	// 深い階層を作って走査に時間をかけさせる
	deep := srcDir
	for i := range 30 {
		deep = fmt.Sprintf("%s/d%d", deep, i)
		if err := os.MkdirAll(filepath.FromSlash(deep), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		for j := range 20 {
			path := filepath.FromSlash(fmt.Sprintf("%s/f%d.txt", deep, j))
			if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
	}

	s := local.New("local")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 最初から取り消しておく

	_, err := transfer.Run(ctx, transfer.Options{
		Src:     s,
		Dst:     s,
		SrcPath: srcDir,
		DstDir:  root + "/dst",
		Workers: 2,
		Compare: transfer.DefaultComparePolicy(),
		Retry:   transfer.RetryPolicy{MaxAttempts: 1},
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// 転送元と転送先が同じストレージの場合の動作を確認します。
func TestSameStorageCopy(t *testing.T) {
	root := filepath.ToSlash(t.TempDir())
	s := local.New("local")
	ctx := context.Background()

	srcDir := root + "/src"
	if _, err := s.Put(ctx, srcDir+"/a.txt", strings.NewReader("内容"),
		storage.ObjectMeta{Size: 6}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	result, err := transfer.Run(ctx, transfer.Options{
		Src:     s,
		Dst:     s,
		SrcPath: srcDir,
		DstDir:  root + "/dst",
		Workers: 1,
		Compare: transfer.DefaultComparePolicy(),
		Retry:   transfer.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 1 {
		t.Errorf("Transferred=%d, want 1", result.Transferred)
	}
}
