package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/mt3hr/hbg/progress"
	"github.com/mt3hr/hbg/storage"
)

// transferWorker はキューからファイルを取り出して転送します。
func (e *engine) transferWorker(ctx context.Context, tasks <-chan task) error {
	for t := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.transferOne(ctx, t)
	}
	return nil
}

// transferOne は1ファイルを転送します。
//
// 失敗しても error を返しません。個々のファイルの失敗で全体を止めず、
// 集計してあとでまとめて報告するためです。
func (e *engine) transferOne(ctx context.Context, t task) {
	dstPath := path.Join(t.dstDir, t.name)
	started := time.Now()

	if e.opts.DryRun {
		e.reporter.Logf("転送する（実行しない）: %s:%s -> %s:%s",
			e.opts.Src.Type(), t.srcPath, e.opts.Dst.Type(), dstPath)

		// 運びはしないが、片付いたことは伝える。
		// 伝えないと、走査で数えたぶんだけ進みぐあいが足りないままになる。
		// 読み取りが起きないので、速度には数えられない。
		tracker := e.reporter.StartFile(t.name, t.size)
		tracker.Finish()

		e.recordSuccess(0)
		e.notify(TransferEvent{SrcPath: t.srcPath, DstPath: dstPath, Duration: time.Since(started)})
		return
	}

	tracker := e.reporter.StartFile(t.name, t.size)
	defer tracker.Finish()

	// 再試行のたびに最初から読み直す。
	// 以前は転送元から受け取った読み取り口を一度しか使えず、
	// そもそも書き込みの再試行ができなかった。
	res := doWithRetry(ctx, e.opts.Retry,
		func(ctx context.Context, _ int) error {
			return e.copyOne(ctx, tracker, t.srcPath, dstPath)
		},
		func(attempt int, wait time.Duration, err error) {
			// 進捗の表示も巻き戻す。
			tracker.Reset()
			e.reporter.Logf("%s ... 失敗 (%v) → %s待機 → 再試行 %d/%d",
				t.name, shortError(err), wait.Round(time.Second), attempt, e.opts.Retry.MaxAttempts)
		})

	elapsed := time.Since(started)

	switch {
	case res.err == nil:
		e.recordSuccess(t.size)

	case errors.Is(res.err, context.Canceled), errors.Is(res.err, context.DeadlineExceeded):
		// 取り消しは失敗として数えない。利用者の意思なので。
		tracker.Abort()

	default:
		tracker.Abort()
		err := fmt.Errorf("%s:%s -> %s:%s: %w",
			e.opts.Src.Type(), t.srcPath, e.opts.Dst.Type(), dstPath, res.err)
		e.recordFailure(err)
		e.reporter.Logf("失敗: %v", err)
	}

	e.notify(TransferEvent{
		SrcPath:  t.srcPath,
		DstPath:  dstPath,
		Bytes:    t.size,
		Duration: elapsed,
		Attempts: res.attempts,
		Err:      res.err,
	})
}

// copyOne は1回ぶんの転送を行います。
func (e *engine) copyOne(ctx context.Context, tracker progress.FileTracker, srcPath, dstPath string) error {
	if err := e.limits.wait(ctx, e.opts.Src); err != nil {
		return err
	}

	_, err := storage.Copy(ctx, e.opts.Src, srcPath, e.opts.Dst, dstPath, storage.CopyOptions{
		VerifyHash: e.verifyHash,
		Wrap: func(r io.Reader) io.Reader {
			// 進捗の計測と帯域の制限を、読み取りの流れに割り込ませる。
			// ストレージの実装はどちらのことも知らない。
			//
			// 帯域制限を内側に置くのは、待ち時間を計測に含めるため。
			// 外側に置くと待っている間が測られず、表示される速度が
			// 実際より何倍も速くなってしまう。
			return tracker.Wrap(e.bw.wrap(ctx, r))
		},
	})
	return err
}

// notify は1ファイルの結果を呼び出し側へ伝えます。
func (e *engine) notify(ev TransferEvent) {
	if e.opts.OnTransfer != nil {
		e.opts.OnTransfer(ev)
	}
}

// shortError は再試行の通知に載せる短い理由を返します。
func shortError(err error) string {
	if err == nil {
		return ""
	}
	if class := storage.ClassOf(err); class == storage.ClassRateLimit {
		return "要求が多すぎます"
	}

	msg := err.Error()
	const limit = 60
	if len(msg) > limit {
		return msg[:limit] + "..."
	}
	return msg
}
