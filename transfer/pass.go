package transfer

import (
	"context"
	"errors"
	"time"
)

// PassPolicy は実行全体をやり直す設定です。
//
// ファイル単位の再試行（RetryPolicy）で拾いきれなかったぶんを、
// 間を置いてもう一度まとめて試します。すでに転送済みのものは
// 差分の判断でスキップされるので、実質的には失敗したぶんだけの
// やり直しになります。
type PassPolicy struct {
	// MaxPasses は最大の実行回数です。1 なら再実行しません。
	MaxPasses int
	// Wait は再実行までの待ち時間です。
	Wait time.Duration
}

// DefaultPassPolicy は既定の設定です。再実行しません。
func DefaultPassPolicy() PassPolicy {
	return PassPolicy{MaxPasses: 1, Wait: time.Minute}
}

// PassReporter は実行のやり直しを利用者に伝えます。
type PassReporter interface {
	// PassStarted は何回目の実行かを伝えます。
	PassStarted(pass, maxPasses int)
	// PassRetrying は、失敗が残っているので待ってやり直すことを伝えます。
	PassRetrying(failed int, wait time.Duration)
}

// RunWithPasses は転送を実行し、失敗が残っていればやり直します。
func RunWithPasses(ctx context.Context, opts Options, pass PassPolicy, reporter PassReporter) (*Result, error) {
	maxPasses := max(pass.MaxPasses, 1)

	total := &Result{}
	var lastErr error

	for i := 1; i <= maxPasses; i++ {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if reporter != nil && maxPasses > 1 {
			reporter.PassStarted(i, maxPasses)
		}

		result, err := Run(ctx, opts)
		if result != nil {
			total.accumulate(result, i)
		}
		lastErr = err

		// 全体を止める種類の失敗（走査の失敗、取り消し）はやり直さない。
		if err != nil {
			return total, err
		}
		if result == nil || result.Failed == 0 {
			return total, nil
		}
		if result.Aborted {
			// 失敗が多すぎて中断した場合、やり直しても同じになりやすい。
			return total, nil
		}
		if i == maxPasses {
			break
		}

		// 1件も転送できなかった場合、やり直しても状況は変わらない。
		// 回数が残っていても打ち切る（無限にやり直さないため）。
		if result.Transferred == 0 {
			break
		}

		if reporter != nil {
			reporter.PassRetrying(result.Failed, pass.Wait)
		}
		if err := sleepCtx(ctx, pass.Wait); err != nil {
			return total, err
		}
	}

	if lastErr != nil && !errors.Is(lastErr, context.Canceled) {
		return total, lastErr
	}
	return total, nil
}

// accumulate は1回ぶんの結果を合算します。
//
// 失敗の一覧は最後の実行のものだけを残します。
// やり直して成功したものが失敗として残ると紛らわしいためです。
func (r *Result) accumulate(other *Result, pass int) {
	r.Transferred += other.Transferred
	r.Skipped += other.Skipped
	r.Bytes += other.Bytes
	r.BytesSkipped += other.BytesSkipped
	r.Elapsed += other.Elapsed
	r.Aborted = other.Aborted

	// 失敗の数と内容は最新のものに置き換える。
	r.Failed = other.Failed
	r.Errors = other.Errors

	_ = pass
}
