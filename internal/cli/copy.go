package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mt3hr/hbg/internal/hbglog"
	"github.com/mt3hr/hbg/progress"
	"github.com/mt3hr/hbg/transfer"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	copyCmd = &cobra.Command{
		Aliases: []string{"cp"},
		RunE:    runCopy,
		Args:    cobra.ExactArgs(2),
		Use:     "copy srcStorage:srcPath destStorage:destDirPath",
		Short:   "ストレージからストレージへとデータをコピーする",
		Long: `ストレージからストレージへとデータをコピーします。
最終更新時刻の差が update_duration 以内で、かつサイズが同じファイルは
スキップされます。

` + supportedStorageTypesHelp() + `
ストレージは設定ファイルで名前を付けて定義します。
同じ種別に別々の名前を与えると、複数のアカウントを使い分けられます。
クラウドは初回に hbg auth login <名前> で認証してください。

転送に失敗した場合は、--retry で同じファイルを試し直し、
それでも残ったものを --retry-pass でまとめて試し直せます。`,
		Example: `使用例
hbg copy local:C:/hoge/test.txt dropbox:/hbg
hbg copy dropbox:/hbg/test.txt local:/home/user/documents
hbg copy -w 10 local:C:/hoge local:C:/fuga
hbg copy --dry-run local:C:/hoge dropbox:/hbg
hbg copy --retry 3 --retry-wait 5s --retry-pass 2 local:C:/hoge dropbox:/hbg
`,
		PreRunE: func(_ *cobra.Command, args []string) error {
			srcInfo, destInfo := args[0], args[1]

			// コロンで区切って、前がstorageの名前、後がpath
			srcSplit := strings.SplitN(srcInfo, ":", 2)
			if len(srcSplit) < 2 {
				return withExitCode(ExitUsage, fmt.Errorf("srcpathの記述が変です: %q（storage:path の形式で指定してください）", srcInfo))
			}
			copyOpt.srcStorage = srcSplit[0]
			copyOpt.srcPath = srcSplit[1]

			destSplit := strings.SplitN(destInfo, ":", 2)
			if len(destSplit) < 2 {
				return withExitCode(ExitUsage, fmt.Errorf("destpathの記述が変です: %q（storage:path の形式で指定してください）", destInfo))
			}
			copyOpt.destStorage = destSplit[0]
			copyOpt.destDirPath = destSplit[1]

			if copyOpt.worker == 0 {
				copyOpt.worker = config.DefaultWorker
			}
			// 設定ファイルに DefaultWorker がない場合、以前はここが 0 のままとなり、
			// 容量0のチャネルとワーカー0個で永久にブロックしていた。
			if copyOpt.worker < 1 {
				copyOpt.worker = defaultWorker
			}
			return nil
		},
	}

	copyOpt = &struct {
		srcStorage  string
		srcPath     string
		destStorage string
		destDirPath string

		worker int

		// 比較の指定
		updateDuration time.Duration
		modifyWindow   time.Duration
		compare        string
		checksum       bool
		sizeOnly       bool
		update         bool
		overwrite      bool
		ignoreExisting bool
		verify         string

		// 絞り込みの指定
		ignore  []string
		include []string
		exclude []string
		minSize string
		maxSize string

		retry         int
		retryWait     time.Duration
		retryBackoff  bool
		retryPass     int
		retryPassWait time.Duration

		tps       float64
		bwLimit   string
		dryRun    bool
		maxErrors int

		progress     string
		progressBars int
		stats        time.Duration
		quiet        bool
		jsonOut      bool
	}{}
)

// defaultIgnores は既定で無視するファイル名です。
var defaultIgnores = []string{
	".nomedia",
	"desktop.ini",
	"thumbnails",
	".thumbnails",
	"Thumbs.db",
	".DS_Store",
	".localized",
}

// registerTransferFlags は copy と check で共通のフラグを登録します。
//
// 両方から同じ指定を受け付けたいので1箇所にまとめています。
// コマンドごとの init に書き分けると、実行順序によっては
// 片方でフラグが登録されないままになります。
func registerTransferFlags(fs *pflag.FlagSet) {
	fs.IntVarP(&copyOpt.worker, "worker", "w", 0, "同時処理数。0だとconfigファイルの値で動きます。")

	fs.StringVar(&copyOpt.compare, "compare", "size,modtime",
		"比較に使う項目 (size, modtime, hash をカンマ区切りで)")
	fs.BoolVar(&copyOpt.checksum, "checksum", false, "内容のハッシュで比較する (--compare size,hash と同じ)")
	fs.BoolVar(&copyOpt.sizeOnly, "size-only", false, "サイズだけで比較する (--compare size と同じ)")
	fs.DurationVar(&copyOpt.modifyWindow, "modify-window", 0,
		"この時間以内の更新時刻の差は同一とみなす（0で自動）")
	fs.DurationVar(&copyOpt.updateDuration, "update_duration", 0,
		"--modify-window の古い名前")
	fs.BoolVar(&copyOpt.update, "update", true, "コピー先のほうが新しい場合は上書きしない")
	fs.BoolVar(&copyOpt.overwrite, "overwrite", false, "コピー先のほうが新しくても上書きする")
	fs.BoolVar(&copyOpt.ignoreExisting, "ignore-existing", false, "コピー先にあるものは内容を問わず転送しない")
	fs.StringVar(&copyOpt.verify, "verify", "auto",
		"転送後の内容の検証 (auto, always, never)")

	fs.StringArrayVarP(&copyOpt.ignore, "ignore", "i", defaultIgnores, "無視するファイル名（完全一致）")
	fs.StringArrayVar(&copyOpt.include, "include", nil, "このパターンに一致するものだけを転送する")
	fs.StringArrayVar(&copyOpt.exclude, "exclude", nil, "このパターンに一致するものを転送しない")
	fs.BoolVar(&copyOpt.jsonOut, "json", false,
		"結果を1行1件の JSON で標準出力へ流す（人向けの表示は標準エラーへ）")

	fs.StringVar(&copyOpt.minSize, "min-size", "", "これより小さいファイルを転送しない（例: 1M）")
	fs.StringVar(&copyOpt.maxSize, "max-size", "", "これより大きいファイルを転送しない（例: 1G）")

	// 古い名前は残すが、案内では出さない。
	_ = fs.MarkDeprecated("update_duration", "--modify-window を使ってください")

	fs.IntVar(&copyOpt.retry, "retry", 3, "1ファイルの転送に失敗したときの再試行回数（0で無効）")
	fs.DurationVar(&copyOpt.retryWait, "retry-wait", 5*time.Second, "再試行までの待ち時間")
	fs.BoolVar(&copyOpt.retryBackoff, "retry-backoff", false, "再試行の待ち時間を試行ごとに伸ばす")
	fs.IntVar(&copyOpt.retryPass, "retry-pass", 0, "失敗が残っていたときに全体をやり直す回数（0で無効）")
	fs.DurationVar(&copyOpt.retryPassWait, "retry-pass-wait", time.Minute, "全体をやり直すまでの待ち時間")

	fs.Float64Var(&copyOpt.tps, "tps", 0, "1秒あたりのAPI呼び出し回数の上限（0で無制限）")
	fs.StringVar(&copyOpt.bwLimit, "bwlimit", "", "転送速度の上限（例: 10M, 512K）")
	fs.BoolVar(&copyOpt.dryRun, "dry-run", false, "実際には転送せず、何が転送されるかだけを表示する")
	fs.IntVar(&copyOpt.maxErrors, "max-errors", 0, "この件数を超えて失敗したら中断する（0で無制限）")

	fs.StringVar(&copyOpt.progress, "progress", "auto",
		"進捗の表示 (auto, always, never, none)")
	fs.IntVar(&copyOpt.progressBars, "progress-bars", 0,
		"同時に表示するファイルごとのバーの本数（0で既定値）")
	fs.DurationVar(&copyOpt.stats, "stats", 30*time.Second,
		"進捗バーを使わないときに集計を表示する間隔（0で表示しない）")
	fs.BoolVarP(&copyOpt.quiet, "quiet", "q", false, "進捗を表示しない")
}

func init() {
	registerTransferFlags(copyCmd.Flags())
}

func runCopy(cmd *cobra.Command, _ []string) error {
	return runTransfer(cmd, false)
}

// runTransfer は copy と sync の本体です。
//
// 違いは、コピー元にないものをコピー先から消すかどうかだけです。
func runTransfer(cmd *cobra.Command, deleteExtraneous bool) error {
	ctx := cmd.Context()

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	// コピー元とコピー先だけが組み立てられる。
	// 設定にある他のストレージの認証は走らない。
	srcStorage, err := resolver.Get(ctx, copyOpt.srcStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	destStorage, err := resolver.Get(ctx, copyOpt.destStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	bwLimit, err := parseByteSize(copyOpt.bwLimit)
	if err != nil {
		return withExitCode(ExitUsage, fmt.Errorf("--bwlimit の指定が不正です: %w", err))
	}

	reporter, err := newReporter()
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer reporter.Close()

	compare, err := buildComparePolicy()
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	filter, err := buildFilter()
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	verify, ok := transfer.ParseVerifyMode(copyOpt.verify)
	if !ok {
		return withExitCode(ExitUsage, fmt.Errorf("--verify の指定が不正です: %q（%s のいずれか）",
			copyOpt.verify, strings.Join(transfer.VerifyModeNames(), ", ")))
	}

	opts := transfer.Options{
		Src:     srcStorage,
		Dst:     destStorage,
		SrcPath: copyOpt.srcPath,
		DstDir:  copyOpt.destDirPath,
		Workers: copyOpt.worker,
		Compare: compare,
		Filter:  filter,
		Verify:  verify,
		Retry: transfer.RetryPolicy{
			MaxAttempts: copyOpt.retry + 1, // 初回 + 再試行回数
			Wait:        copyOpt.retryWait,
			Backoff:     copyOpt.retryBackoff,
			MaxWait:     5 * time.Minute,
		},
		TPS:             copyOpt.tps,
		BandwidthLimit:  bwLimit,
		Delete:          deleteExtraneous,
		DeleteOnPartial: deleteExtraneous && syncOpt.deleteOnPartial,
		DryRun:          copyOpt.dryRun,
		MaxErrors:       copyOpt.maxErrors,
		Reporter:        reporter,
		OnTransfer:      logTransferEvent(srcStorage.Type(), destStorage.Type()),
	}

	// --json のときは、機械向けの出力を標準出力へ流す。
	// 人向けのまとめは出さない。混ざると読み取れなくなるため。
	var jsonw *jsonWriter
	if copyOpt.jsonOut {
		jsonw = newJSONWriter(os.Stdout)

		logEvent := opts.OnTransfer
		emitEvent := jsonw.onTransfer(srcStorage.Name(), destStorage.Name())
		opts.OnTransfer = func(ev transfer.TransferEvent) {
			logEvent(ev)
			emitEvent(ev)
		}
		opts.OnDecision = jsonw.onDecision()
	}

	pass := transfer.PassPolicy{
		MaxPasses: copyOpt.retryPass + 1, // 初回 + やり直し回数
		Wait:      copyOpt.retryPassWait,
	}

	result, err := transfer.RunWithPasses(ctx, opts, pass, &passReporter{r: reporter})

	// まとめを書く前に表示を閉じる。
	//
	// 進捗は標準エラー、まとめは標準出力へ書くので、閉じずに書くと
	// 端末では進捗バーの描き直しとまとめが混ざってしまう。
	// Close は二度呼んでも害がないので、後始末の defer はそのまま残す。
	_ = reporter.Close()

	if result != nil {
		hbglog.LogSummary(result.Transferred, result.Failed, result.Elapsed)
		if jsonw != nil {
			jsonw.summary(result)
		} else {
			writeSummary(os.Stdout, result)
		}
	}

	if err != nil {
		if isCanceled(err) {
			return withExitCode(ExitInterrupted, fmt.Errorf("中断しました"))
		}
		return fmt.Errorf("error at copy file from %s:%s to %s:%s: %w",
			srcStorage.Type(), copyOpt.srcPath, destStorage.Type(), copyOpt.destDirPath, err)
	}

	switch {
	case result.Failed > 0:
		return withExitCode(ExitTransferFailed,
			fmt.Errorf("%d件のファイルのコピーに失敗しました", result.Failed))
	case result.DeleteFailed > 0:
		return withExitCode(ExitTransferFailed,
			fmt.Errorf("%d件の削除に失敗しました", result.DeleteFailed))
	}
	return nil
}

// logTransferEvent は転送1件ごとにログを残す関数を返します。
func logTransferEvent(srcType, dstType string) func(transfer.TransferEvent) {
	return func(ev transfer.TransferEvent) {
		rec := hbglog.TransferRecord{
			SrcStorage: srcType,
			SrcPath:    ev.SrcPath,
			DstStorage: dstType,
			DstPath:    ev.DstPath,
			Bytes:      ev.Bytes,
			Duration:   ev.Duration,
			Result:     hbglog.ResultCopied,
			Err:        ev.Err,
		}
		switch {
		case ev.Err != nil:
			rec.Result = hbglog.ResultFailed
		case ev.Skipped:
			rec.Result = hbglog.ResultSkipped
		}
		hbglog.LogTransfer(rec)
	}
}

// passReporter は全体のやり直しを利用者に伝えます。
type passReporter struct {
	r progress.Reporter
}

func (p *passReporter) PassStarted(pass, maxPasses int) {
	p.r.Logf("[pass %d/%d]", pass, maxPasses)
}

func (p *passReporter) PassRetrying(failed int, wait time.Duration) {
	p.r.Logf("%d件失敗しました。%s待機して再実行します...", failed, wait.Round(time.Second))
}
