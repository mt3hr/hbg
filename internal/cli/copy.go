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

		worker         int
		updateDuration time.Duration
		ignore         []string

		retry         int
		retryWait     time.Duration
		retryBackoff  bool
		retryPass     int
		retryPassWait time.Duration

		tps       float64
		bwLimit   string
		dryRun    bool
		maxErrors int
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

func init() {
	fs := copyCmd.Flags()
	fs.StringArrayVarP(&copyOpt.ignore, "ignore", "i", defaultIgnores, "無視するファイル")
	fs.DurationVar(&copyOpt.updateDuration, "update_duration", time.Second, "更新されたとみなす期間")
	fs.IntVarP(&copyOpt.worker, "worker", "w", 0, "同時処理数。0だとconfigファイルの値で動きます。")

	fs.IntVar(&copyOpt.retry, "retry", 3, "1ファイルの転送に失敗したときの再試行回数（0で無効）")
	fs.DurationVar(&copyOpt.retryWait, "retry-wait", 5*time.Second, "再試行までの待ち時間")
	fs.BoolVar(&copyOpt.retryBackoff, "retry-backoff", false, "再試行の待ち時間を試行ごとに伸ばす")
	fs.IntVar(&copyOpt.retryPass, "retry-pass", 0, "失敗が残っていたときに全体をやり直す回数（0で無効）")
	fs.DurationVar(&copyOpt.retryPassWait, "retry-pass-wait", time.Minute, "全体をやり直すまでの待ち時間")

	fs.Float64Var(&copyOpt.tps, "tps", 0, "1秒あたりのAPI呼び出し回数の上限（0で無制限）")
	fs.StringVar(&copyOpt.bwLimit, "bwlimit", "", "転送速度の上限（例: 10M, 512K）")
	fs.BoolVar(&copyOpt.dryRun, "dry-run", false, "実際には転送せず、何が転送されるかだけを表示する")
	fs.IntVar(&copyOpt.maxErrors, "max-errors", 0, "この件数を超えて失敗したら中断する（0で無制限）")
}

func runCopy(cmd *cobra.Command, _ []string) error {
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

	reporter := newReporter()
	defer reporter.Close()

	opts := transfer.Options{
		Src:     srcStorage,
		Dst:     destStorage,
		SrcPath: copyOpt.srcPath,
		DstDir:  copyOpt.destDirPath,
		Workers: copyOpt.worker,
		Compare: transfer.ComparePolicy{ModifyWindow: copyOpt.updateDuration},
		Ignore:  copyOpt.ignore,
		Retry: transfer.RetryPolicy{
			MaxAttempts: copyOpt.retry + 1, // 初回 + 再試行回数
			Wait:        copyOpt.retryWait,
			Backoff:     copyOpt.retryBackoff,
			MaxWait:     5 * time.Minute,
		},
		TPS:            copyOpt.tps,
		BandwidthLimit: bwLimit,
		DryRun:         copyOpt.dryRun,
		MaxErrors:      copyOpt.maxErrors,
		Reporter:       reporter,
		OnTransfer:     logTransferEvent(srcStorage.Type(), destStorage.Type()),
	}

	pass := transfer.PassPolicy{
		MaxPasses: copyOpt.retryPass + 1, // 初回 + やり直し回数
		Wait:      copyOpt.retryPassWait,
	}

	result, err := transfer.RunWithPasses(ctx, opts, pass, &passReporter{r: reporter})
	if result != nil {
		hbglog.LogSummary(result.Transferred, result.Failed, result.Elapsed)
		writeSummary(os.Stdout, result)
	}

	if err != nil {
		if isCanceled(err) {
			return withExitCode(ExitInterrupted, fmt.Errorf("中断しました"))
		}
		return fmt.Errorf("error at copy file from %s:%s to %s:%s: %w",
			srcStorage.Type(), copyOpt.srcPath, destStorage.Type(), copyOpt.destDirPath, err)
	}

	if result.Failed > 0 {
		return withExitCode(ExitTransferFailed,
			fmt.Errorf("%d件のファイルのコピーに失敗しました", result.Failed))
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
