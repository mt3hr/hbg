package transfer

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// RetryPolicy はファイル単位の再試行の設定です。
type RetryPolicy struct {
	// MaxAttempts は最大の試行回数です。1 なら再試行しません。
	MaxAttempts int
	// Wait は再試行までの待ち時間です。
	Wait time.Duration
	// Backoff を真にすると、待ち時間を試行ごとに倍にします（ゆらぎ付き）。
	// 偽なら Wait のまま一定です。
	Backoff bool
	// MaxWait は待ち時間の上限です。Backoff のときに使います。
	MaxWait time.Duration
}

// DefaultRetryPolicy は既定の再試行の設定です。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Wait:        5 * time.Second,
		Backoff:     false,
		MaxWait:     time.Minute,
	}
}

// waitFor は attempt 回目の失敗のあとに待つ時間を返します。
//
// サーバーから待ち時間を指示されている場合はそちらを優先します。
// 指示を無視して短い間隔で叩き直すと、制限の対象となる要求が増えるだけで
// かえって遅くなります。
func (p RetryPolicy) waitFor(attempt int, err error) time.Duration {
	wait := p.Wait

	if p.Backoff {
		// 2の累乗で伸ばす。同時に失敗した転送が足並みを揃えて
		// 再試行しないよう、ゆらぎを加える。
		for i := 1; i < attempt; i++ {
			wait *= 2
			if p.MaxWait > 0 && wait > p.MaxWait {
				wait = p.MaxWait
				break
			}
		}
		if wait > 0 {
			jitter := time.Duration(rand.Int64N(int64(wait) / 2))
			wait = wait/2 + jitter
		}
	}

	if after := storage.RetryAfterOf(err); after > wait {
		wait = after
	}
	if p.MaxWait > 0 && wait > p.MaxWait {
		// サーバーの指示であっても、極端に長い場合は上限で止める。
		if storage.RetryAfterOf(err) == 0 {
			wait = p.MaxWait
		}
	}
	return wait
}

// retryableError は、この失敗を再試行してよいかを返します。
func retryableError(err error) bool {
	if err == nil {
		return false
	}
	// 取り消しは利用者の意思なので再試行しない。
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return storage.ClassOf(err).Retryable()
}

// attemptResult は1回の試行の結果です。
type attemptResult struct {
	err      error
	attempts int
}

// doWithRetry は fn を実行し、失敗したら設定に従って再試行します。
//
// onRetry は再試行の直前に呼ばれます。進捗表示の巻き戻しや
// 利用者への通知に使います。
func doWithRetry(
	ctx context.Context,
	p RetryPolicy,
	fn func(ctx context.Context, attempt int) error,
	onRetry func(attempt int, wait time.Duration, err error),
) attemptResult {
	maxAttempts := max(p.MaxAttempts, 1)

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return attemptResult{err: ctxErr, attempts: attempt - 1}
		}

		err = fn(ctx, attempt)
		if err == nil {
			return attemptResult{attempts: attempt}
		}

		// 待っても直らない失敗（存在しない、権限がない、認証が必要）は
		// ここで打ち切る。3回 × 5秒 待って諦めるのは無駄なので。
		if !retryableError(err) {
			return attemptResult{err: err, attempts: attempt}
		}
		if attempt == maxAttempts {
			break
		}

		wait := p.waitFor(attempt, err)
		if onRetry != nil {
			onRetry(attempt, wait, err)
		}
		if waitErr := sleepCtx(ctx, wait); waitErr != nil {
			return attemptResult{err: waitErr, attempts: attempt}
		}
	}
	return attemptResult{err: err, attempts: maxAttempts}
}

// sleepCtx は ctx が取り消されたら途中で戻る待機です。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
