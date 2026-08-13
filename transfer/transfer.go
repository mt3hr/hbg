// Package transfer はストレージ間のファイル転送を行います。
//
// 以前の実装は、まず全ツリーを走査してコピー対象を残らずメモリに
// 溜め、それが終わってから転送を始める2段構えでした。そのため
// 巨大なツリーでは数分間なにも表示されないうえ、対象の一覧を
// 丸ごと保持するので使用メモリも件数に比例して増えていました。
//
// ここでは走査と転送を並行させます。見つかったものから順に転送を
// 始めるので待ち時間の無音がなくなり、保持するのは処理中の
// ディレクトリぶんだけなので使用メモリが件数によらず一定になります。
package transfer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/mt3hr/hbg/progress"
	"github.com/mt3hr/hbg/storage"
)

// Options は転送の設定です。
type Options struct {
	// Src と Dst は転送元と転送先のストレージです。
	Src storage.Storage
	Dst storage.Storage
	// SrcPath は転送元のパスです。ファイルでもディレクトリでも構いません。
	SrcPath string
	// DstDir は転送先のディレクトリです。
	DstDir string

	// Workers は同時に転送するファイル数です。1未満なら1にします。
	Workers int

	// Compare は転送の要否を判断する規則です。
	Compare ComparePolicy
	// Filter は転送対象の絞り込みです。nil ならすべて対象。
	Filter *Filter

	// Verify は転送後の内容の検証方法です。
	Verify VerifyMode

	// Retry はファイル単位の再試行の設定です。
	Retry RetryPolicy

	// TPS は1秒あたりのAPI呼び出し回数の上限です。0 なら無制限。
	TPS float64
	// TPSPerType は種別ごとの上限です。TPS より優先されます。
	TPSPerType map[string]float64
	// BandwidthLimit は1秒あたりの転送バイト数の上限です。0 なら無制限。
	BandwidthLimit int64

	// Delete を真にすると、コピー元にないものをコピー先から消します。
	//
	// 転送に1件でも失敗があれば削除は行いません。読めなかったものを
	// 「向こうには無い」と取り違えて消してしまわないためです。
	Delete bool

	// DryRun を真にすると、実際には転送せず何が転送されるかだけを示します。
	DryRun bool

	// MaxErrors はこの件数を超えて失敗したら中断します。0 なら中断しません。
	MaxErrors int

	// Reporter は進みぐあいの表示先です。nil なら何も表示しません。
	Reporter progress.Reporter

	// OnTransfer は1ファイルの処理が終わるたびに呼ばれます。
	// ログの記録に使います。
	OnTransfer func(TransferEvent)

	// OnDecision は転送の要否を判断するたびに呼ばれます。
	// hbg check のように、判断だけを一覧したい場合に使います。
	OnDecision func(DecisionEvent)
}

// VerifyMode は転送後の検証方法です。
type VerifyMode string

const (
	// VerifyAuto は、追加の入出力が要らない場合にだけ検証します。
	VerifyAuto VerifyMode = "auto"
	// VerifyAlways は必ず検証します。
	VerifyAlways VerifyMode = "always"
	// VerifyNever は検証しません（サイズの一致は常に確かめます）。
	VerifyNever VerifyMode = "never"
)

// ParseVerifyMode は文字列から検証方法を求めます。
func ParseVerifyMode(s string) (VerifyMode, bool) {
	switch VerifyMode(s) {
	case VerifyAuto, VerifyAlways, VerifyNever:
		return VerifyMode(s), true
	}
	return "", false
}

// VerifyModeNames は指定できる値を返します。
func VerifyModeNames() []string {
	return []string{string(VerifyAuto), string(VerifyAlways), string(VerifyNever)}
}

// DecisionEvent は1ファイルの転送要否の判断です。
type DecisionEvent struct {
	// Path はコピー元の起点からの相対パスです。
	Path string
	Size int64
	// Action は転送するかどうかです。
	Action Action
	// Reason は判断の理由です。
	Reason string
}

// TransferEvent は1ファイルの処理結果です。
type TransferEvent struct {
	SrcPath  string
	DstPath  string
	Bytes    int64
	Duration time.Duration
	Attempts int
	Skipped  bool
	Err      error
}

// Result は転送全体の結果です。
type Result struct {
	Transferred  int
	Skipped      int
	Failed       int
	Bytes        int64
	BytesSkipped int64
	Elapsed      time.Duration

	// Deleted はコピー元にないため消した件数です。
	Deleted int
	// DeleteFailed は削除に失敗した件数です。
	DeleteFailed int

	// Errors は表示用に保持する失敗の詳細です。
	// MaxReportedErrors 件で打ち切られます。
	Errors []error
	// Aborted は MaxErrors に達して中断したことを表します。
	Aborted bool
}

// MaxReportedErrors はサマリに残す失敗の最大件数です。
const MaxReportedErrors = 20

// task は1ファイルの転送指示です。
type task struct {
	srcPath string
	dstDir  string
	name    string
	relPath string
	size    int64
}

// engine は1回の転送の状態です。
type engine struct {
	opts     Options
	reporter progress.Reporter
	limits   *limiterSet
	bw       *bandwidthLimiter
	comparer *Comparer
	// verifyHash は転送後の検証に使うハッシュです。使わない場合は空です。
	verifyHash storage.HashType

	// 走査の途中経過
	scanDirs  atomic.Int64
	scanFiles atomic.Int64
	scanBytes atomic.Int64

	// 結果
	mu     sync.Mutex
	result Result

	// 作成済みのディレクトリ。同じディレクトリを何度も作らないため。
	dirsMu   sync.Mutex
	madeDirs map[string]struct{}

	// コピー元にないのにコピー先にあるもの。
	// 転送がすべて終わってから消します。
	extraMu sync.Mutex
	extra   []extraneous

	// abort は中断を伝えます。
	abort context.CancelFunc
}

// Run は転送を実行します。
func Run(ctx context.Context, opts Options) (*Result, error) {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.Retry.MaxAttempts < 1 {
		opts.Retry.MaxAttempts = 1
	}
	if opts.Reporter == nil {
		opts.Reporter = progress.NewNop()
	}
	if opts.Verify == "" {
		opts.Verify = VerifyAuto
	}
	if len(opts.Compare.Fields) == 0 {
		opts.Compare = DefaultComparePolicy()
	}

	comparer, err := NewComparer(opts.Compare, opts.Src, opts.Dst)
	if err != nil {
		return nil, err
	}

	// 転送元が存在しなければ、ここで失敗させる。
	// 以前は一致するものがなくても「0件成功」で正常終了しており、
	// パスを打ち間違えてもスクリプトからは成功に見えていた。
	srcInfo, statErr := opts.Src.Stat(ctx, opts.SrcPath)
	if statErr != nil {
		return nil, fmt.Errorf("コピー元を確認できません %s:%s: %w", opts.Src.Type(), opts.SrcPath, statErr)
	}

	// 呼び出し側の ctx と、中断のために自分で作る ctx を分けておく。
	// 「利用者が中断した」のか「失敗が多すぎて自分で止めた」のかを
	// 区別する必要があるため。
	callerCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	e := &engine{
		opts:     opts,
		reporter: opts.Reporter,
		limits:   newLimiterSet(opts.TPS, opts.TPSPerType),
		bw:       newBandwidthLimiter(opts.BandwidthLimit),
		madeDirs: map[string]struct{}{},
		abort:    cancel,
		comparer: comparer,
	}
	e.verifyHash = resolveVerifyHash(opts, comparer)

	started := time.Now()
	e.reporter.ScanStarted()

	// 走査と転送を同時に動かす。
	// 走査が終わるのを待たずに転送が始まるので、
	// 待ち時間の無音がなくなる。
	tasks := make(chan task, opts.Workers*2)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(tasks)
		return e.scan(gctx, *srcInfo, tasks)
	})

	for range opts.Workers {
		g.Go(func() error {
			return e.transferWorker(gctx, tasks)
		})
	}

	waitErr := g.Wait()

	// 利用者が中断した場合は、たとえ処理中のものが偶然すべて
	// 終わっていても、中断として報告する。
	// 呼び出し側が終了コードを正しく決められるようにするため。
	if waitErr == nil && callerCtx.Err() != nil {
		waitErr = callerCtx.Err()
	}

	// 削除は転送がすべて終わってから行う。
	// 途中で消すと、まだ判断していないものまで消しかねない。
	if waitErr == nil {
		e.deleteExtraneous(ctx)
	}

	e.mu.Lock()
	result := e.result
	e.mu.Unlock()

	result.Elapsed = time.Since(started)
	e.reporter.ScanDone(e.scanDirs.Load(), e.scanFiles.Load(), e.scanBytes.Load())
	e.reporter.Done(progress.Summary{
		Transferred:  result.Transferred,
		Skipped:      result.Skipped,
		Failed:       result.Failed,
		Bytes:        result.Bytes,
		BytesSkipped: result.BytesSkipped,
		Elapsed:      result.Elapsed,
	})

	// 個々のファイルの失敗は result に集計済み。
	// ここで返るのは走査の失敗や取り消しなど、全体を止める種類のもの。
	if waitErr != nil && !result.Aborted {
		return &result, waitErr
	}
	return &result, nil
}

// resolveVerifyHash は転送後の検証に使うハッシュを決めます。
//
// auto では、追加の入出力なしに検証できる場合だけ使います。
// 転送しながらコピー元のハッシュを計算し、書き込み後に返ってくる
// 値と突き合わせる形なので、余分な読み書きは発生しません。
func resolveVerifyHash(opts Options, comparer *Comparer) storage.HashType {
	switch opts.Verify {
	case VerifyNever:
		return ""
	case VerifyAlways:
		if ht, ok := commonHash(opts.Src, opts.Dst); ok {
			return ht
		}
		return ""
	}

	// auto: 比較にハッシュを使っているなら、それをそのまま検証にも使う。
	if ht := comparer.HashType(); ht != "" {
		return ht
	}
	// 書き込み先がメタデータとしてハッシュを返せる場合も、
	// 追加の入出力なしに検証できる。
	if f := opts.Dst.Features(); f != nil && len(f.Hashes) > 0 {
		if ht, ok := commonHash(opts.Src, opts.Dst); ok {
			return ht
		}
	}
	return ""
}

// notifyDecision は転送要否の判断を呼び出し側へ伝えます。
func (e *engine) notifyDecision(ev DecisionEvent) {
	if e.opts.OnDecision != nil {
		e.opts.OnDecision(ev)
	}
}

// recordSuccess は成功を記録します。
func (e *engine) recordSuccess(bytes int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.result.Transferred++
	e.result.Bytes += bytes
}

// recordSkip はスキップを記録します。
func (e *engine) recordSkip(bytes int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.result.Skipped++
	if bytes > 0 {
		e.result.BytesSkipped += bytes
	}
}

// recordDeleted は削除を記録します。
func (e *engine) recordDeleted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.result.Deleted++
}

// recordDeleteFailure は削除の失敗を記録します。
//
// 転送の失敗とは分けて数えます。転送は成功したのに片付けだけ
// できなかった、という状態を区別できるようにするためです。
func (e *engine) recordDeleteFailure(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.result.DeleteFailed++
	if len(e.result.Errors) < MaxReportedErrors {
		e.result.Errors = append(e.result.Errors, err)
	}
}

// failedCount は転送に失敗した件数を返します。
func (e *engine) failedCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.result.Failed
}

// recordFailure は失敗を記録し、上限に達していれば中断を指示します。
func (e *engine) recordFailure(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.result.Failed++
	if len(e.result.Errors) < MaxReportedErrors {
		e.result.Errors = append(e.result.Errors, err)
	}
	if e.opts.MaxErrors > 0 && e.result.Failed >= e.opts.MaxErrors {
		e.result.Aborted = true
		e.abort()
	}
}
