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

// failNTimes は、最初の n 回だけ失敗する障害を作ります。
func failNTimes(n int32, class storage.Class) (memory.Hooks, *atomic.Int32) {
	var calls atomic.Int32
	hooks := memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op != "put" {
				return nil
			}
			if calls.Add(1) <= n {
				return storage.Wrap("put", "dst", path, class, errors.New("一時的に失敗しました"))
			}
			return nil
		},
	}
	return hooks, &calls
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "内容")

	hooks, calls := failNTimes(2, storage.ClassRetryable)
	dst.SetHooks(hooks)

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 3, Wait: time.Millisecond}

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.Transferred != 1 || result.Failed != 0 {
		t.Errorf("Transferred=%d Failed=%d, want 1 と 0", result.Transferred, result.Failed)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("試行回数 = %d, want 3（2回失敗して3回目で成功）", got)
	}
	if got := dst.Snapshot()["/backup/data/a.txt"]; got != "内容" {
		t.Errorf("内容 = %q", got)
	}
}

func TestRetryGivesUpAfterMaxAttempts(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "内容")

	hooks, calls := failNTimes(100, storage.ClassRetryable)
	dst.SetHooks(hooks)

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 3, Wait: time.Millisecond}

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.Failed != 1 {
		t.Errorf("Failed=%d, want 1", result.Failed)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("試行回数 = %d, want 3（上限まで）", got)
	}
}

// 待っても直らない失敗は再試行しないことを確認します。
//
// 存在しないファイルを3回×5秒かけて諦めるのは無駄なので、
// 分類を見て即座に打ち切ります。
func TestRetrySkipsPermanentFailures(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "内容")

	hooks, calls := failNTimes(100, storage.ClassPermanent)
	dst.SetHooks(hooks)

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 5, Wait: time.Second}

	started := time.Now()
	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.Failed != 1 {
		t.Errorf("Failed=%d, want 1", result.Failed)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("試行回数 = %d, want 1（待っても直らないので再試行しない）", got)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Errorf("所要時間 = %v、再試行の待ち時間が発生している", elapsed)
	}
}

// 取り消しは再試行しないことを確認します。
func TestRetrySkipsCanceled(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "内容")

	ctx, cancel := context.WithCancel(context.Background())

	// 再試行の対象になる失敗を返しつつ、同時に取り消す。
	// 分類だけを見れば再試行するが、取り消されているので試行は打ち切られる。
	var calls atomic.Int32
	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op != "put" {
				return nil
			}
			calls.Add(1)
			cancel()
			return storage.Wrap("put", "dst", path, storage.ClassRetryable, errors.New("失敗"))
		},
	})

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 5, Wait: time.Second}

	result, err := transfer.Run(ctx, opts)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got > 1 {
		t.Errorf("試行回数 = %d, 取り消し後に再試行している", got)
	}
	// 取り消しは失敗として数えない
	if result != nil && result.Failed != 0 {
		t.Errorf("Failed=%d, 取り消しは失敗に数えない", result.Failed)
	}
}

// 待ち時間が指定どおり一定であることを確認します。
// --retry-backoff を指定しない限り、間隔は伸びません。
func TestRetryUsesFixedWaitByDefault(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "内容")

	var timestamps []time.Time
	var calls atomic.Int32
	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op != "put" {
				return nil
			}
			timestamps = append(timestamps, time.Now())
			if calls.Add(1) <= 2 {
				return storage.Wrap("put", "dst", path, storage.ClassRetryable, errors.New("失敗"))
			}
			return nil
		},
	})

	const wait = 50 * time.Millisecond
	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 3, Wait: wait}

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(timestamps) != 3 {
		t.Fatalf("試行回数 = %d, want 3", len(timestamps))
	}
	for i := 1; i < len(timestamps); i++ {
		gap := timestamps[i].Sub(timestamps[i-1])
		// 待ち時間の前後にぶれるので幅を持たせる
		if gap < wait/2 || gap > wait*5 {
			t.Errorf("%d回目の間隔 = %v, おおよそ %v であるべき", i, gap, wait)
		}
	}
}

// 実行全体のやり直しで、残った失敗が拾えることを確認します。
func TestRunWithPassesRecoversFailures(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")
	put(t, src, "/data/b.txt", "2")

	// 1回目の実行では b.txt だけ失敗させ、2回目は成功させる。
	var failing atomic.Bool
	failing.Store(true)
	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "put" && strings.HasSuffix(path, "b.txt") && failing.Load() {
				return storage.Wrap("put", "dst", path, storage.ClassRetryable, errors.New("一時的な失敗"))
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 1}

	reporter := &recordingPassReporter{onRetry: func() { failing.Store(false) }}
	pass := transfer.PassPolicy{MaxPasses: 3, Wait: time.Millisecond}

	result, err := transfer.RunWithPasses(context.Background(), opts, pass, reporter)
	if err != nil {
		t.Fatalf("RunWithPasses: %v", err)
	}

	if result.Failed != 0 {
		t.Errorf("Failed=%d, want 0（やり直しで拾えるはず）", result.Failed)
	}
	if reporter.retries != 1 {
		t.Errorf("やり直しの回数 = %d, want 1", reporter.retries)
	}

	snap := dst.Snapshot()
	if snap["/backup/data/b.txt"] != "2" {
		t.Errorf("b.txt が転送されていない: %q", snap["/backup/data/b.txt"])
	}
}

// やり直しても状況が変わらない場合に打ち切ることを確認します。
// 回数が残っていても、1件も転送できなければ続けても無駄なので止めます。
func TestRunWithPassesStopsWhenNoProgress(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")

	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "put" {
				return storage.Wrap("put", "dst", path, storage.ClassRetryable, errors.New("いつも失敗"))
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 1}

	reporter := &recordingPassReporter{}
	pass := transfer.PassPolicy{MaxPasses: 5, Wait: time.Millisecond}

	started := time.Now()
	result, err := transfer.RunWithPasses(context.Background(), opts, pass, reporter)
	if err != nil {
		t.Fatalf("RunWithPasses: %v", err)
	}

	if result.Failed == 0 {
		t.Error("失敗が記録されていない")
	}
	if reporter.passes > 2 {
		t.Errorf("実行回数 = %d、進捗がないので早く打ち切るべき", reporter.passes)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("所要時間 = %v、やり直しを続けすぎている", elapsed)
	}
}

// すでに転送済みのものは、やり直しでスキップされることを確認します。
func TestRunWithPassesSkipsAlreadyTransferred(t *testing.T) {
	src, dst := newPair(t)
	for i := range 5 {
		put(t, src, fmt.Sprintf("/data/f%d.txt", i), fmt.Sprintf("内容%d", i))
	}

	var failing atomic.Bool
	failing.Store(true)
	dst.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "put" && strings.HasSuffix(path, "f4.txt") && failing.Load() {
				return storage.Wrap("put", "dst", path, storage.ClassRetryable, errors.New("失敗"))
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Workers = 1
	opts.Retry = transfer.RetryPolicy{MaxAttempts: 1}

	reporter := &recordingPassReporter{onRetry: func() { failing.Store(false) }}
	result, err := transfer.RunWithPasses(context.Background(), opts,
		transfer.PassPolicy{MaxPasses: 2, Wait: time.Millisecond}, reporter)
	if err != nil {
		t.Fatalf("RunWithPasses: %v", err)
	}

	// 1回目に4件、2回目に1件。合計5件。
	if result.Transferred != 5 {
		t.Errorf("Transferred=%d, want 5", result.Transferred)
	}
	// 2回目は転送済みの4件がスキップされる
	if result.Skipped != 4 {
		t.Errorf("Skipped=%d, want 4（やり直しで転送済みのぶん）", result.Skipped)
	}
}

// recordingPassReporter はやり直しの回数を数えます。
type recordingPassReporter struct {
	passes  int
	retries int
	onRetry func()
}

func (r *recordingPassReporter) PassStarted(int, int) { r.passes++ }

func (r *recordingPassReporter) PassRetrying(int, time.Duration) {
	r.retries++
	if r.onRetry != nil {
		r.onRetry()
	}
}
