package transfer_test

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mt3hr/hbg/progress"
	"github.com/mt3hr/hbg/transfer"
)

// scanWatcher は走査の終わりが知らされるのを待つ Reporter です。
type scanWatcher struct {
	progress.Nop

	once sync.Once
	done chan struct{}
	// late は待たされたまま知らされなかったことを表します。
	late atomic.Bool
}

func (w *scanWatcher) ScanDone(_, _ int64, _ int64) {
	w.once.Do(func() { close(w.done) })
}

func (w *scanWatcher) StartFile(name string, size int64) progress.FileTracker {
	select {
	case <-w.done:
	case <-time.After(3 * time.Second):
		w.late.Store(true)
	}
	return w.Nop.StartFile(name, size)
}

// 走査が終わった時点でそれが知らされることを確認します。
//
// 以前は転送がすべて終わってから知らされていたので、
// 「調査中」の表示が最後まで消えず、総量も確定しないままでした。
// 総量が決まらないうちは、残り時間は下限にしかなりません。
func TestScanDoneReportedWhenScanEnds(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/b.txt", "bbb")

	watcher := &scanWatcher{done: make(chan struct{})}
	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Reporter = watcher

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if watcher.late.Load() {
		t.Error("転送が終わるまで走査の終わりが知らされなかった")
	}
	if result.Transferred != 2 {
		t.Errorf("Transferred = %d, want 2", result.Transferred)
	}
}

// skipRecorder は転送不要と判断されたぶんを記録する Reporter です。
type skipRecorder struct {
	progress.Nop

	files atomic.Int64
	bytes atomic.Int64
}

func (r *skipRecorder) Skipped(_ string, size int64) {
	r.files.Add(1)
	r.bytes.Add(size)
}

// 転送不要と判断したぶんが、大きさとともに表示側へ伝わることを確認します。
//
// 伝わらないと、すでにコピー済みのものが多いときに
// 進みぐあいがいつまでも上がらず、残り時間も当てになりません。
func TestSkippedReportedWithSize(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/sub/b.txt", "bbbbb")

	if _, err := transfer.Run(context.Background(), baseOptions(src, dst)); err != nil {
		t.Fatalf("1回目: %v", err)
	}

	rec := &skipRecorder{}
	opts := baseOptions(src, dst)
	opts.Reporter = rec
	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("2回目: %v", err)
	}

	if result.Skipped != 2 {
		t.Fatalf("Skipped = %d, want 2", result.Skipped)
	}
	if got := rec.files.Load(); got != 2 {
		t.Errorf("表示へ伝わった件数 = %d, want 2", got)
	}
	if got := rec.bytes.Load(); got != 8 {
		t.Errorf("表示へ伝わった量 = %d, want 8", got)
	}
}

// fileRecorder はファイル単位の記録係の使われ方を数える Reporter です。
type fileRecorder struct {
	progress.Nop

	started  atomic.Int64
	finished atomic.Int64
}

func (r *fileRecorder) StartFile(string, int64) progress.FileTracker {
	r.started.Add(1)
	return &countingTracker{rec: r}
}

type countingTracker struct {
	rec *fileRecorder
}

func (t *countingTracker) Wrap(r io.Reader) io.Reader { return r }
func (t *countingTracker) Reset()                     {}
func (t *countingTracker) Complete(int64)             {}
func (t *countingTracker) Abort()                     {}
func (t *countingTracker) Finish()                    { t.rec.finished.Add(1) }

// --dry-run でも、片付いたことが表示側へ伝わることを確認します。
//
// 伝わらないと、走査で数えたぶんだけ進みぐあいが足りないままになります。
func TestDryRunReportsFiles(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/b.txt", "bbb")

	rec := &fileRecorder{}
	opts := baseOptions(src, dst)
	opts.DryRun = true
	opts.Reporter = rec

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := rec.started.Load(); got != 2 {
		t.Errorf("始まりを伝えた件数 = %d, want 2", got)
	}
	if got := rec.finished.Load(); got != 2 {
		t.Errorf("終わりを伝えた件数 = %d, want 2", got)
	}
}
