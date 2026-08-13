package cli

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"text/tabwriter"

	"github.com/mt3hr/hbg/progress"
	"github.com/mt3hr/hbg/transfer"
	"github.com/spf13/cobra"
)

var checkCmd = &cobra.Command{
	Use:     "check srcStorage:srcPath destStorage:destDirPath",
	Aliases: []string{"diff"},
	Short:   "転送せずに差分を確認する",
	Long: `コピー元とコピー先を比べて、何が転送されるかを一覧します。
実際の転送は行いません。

バックアップが取れているかの確認や、--compare の指定を
決めるときの下調べに使えます。`,
	Example: `使用例
hbg check local:C:/photos dropbox:/backup
hbg check --checksum local:C:/photos dropbox:/backup
hbg check --all local:C:/photos dropbox:/backup
`,
	Args:    cobra.ExactArgs(2),
	RunE:    runCheck,
	PreRunE: copyCmd.PreRunE,
}

var checkOpt = struct {
	all bool
}{}

func init() {
	// copy と同じ比較・絞り込みの指定を受け付ける。
	fs := checkCmd.Flags()
	registerTransferFlags(fs)
	fs.BoolVar(&checkOpt.all, "all", false, "一致しているものも表示する")
}

func runCheck(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	srcStorage, err := resolver.Get(ctx, copyOpt.srcStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	destStorage, err := resolver.Get(ctx, copyOpt.destStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	compare, err := buildComparePolicy()
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	filter, err := buildFilter()
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	report := &checkReport{showAll: checkOpt.all}

	_, err = transfer.Run(ctx, transfer.Options{
		Src:        srcStorage,
		Dst:        destStorage,
		SrcPath:    copyOpt.srcPath,
		DstDir:     copyOpt.destDirPath,
		Workers:    copyOpt.worker,
		Compare:    compare,
		Filter:     filter,
		Retry:      transfer.RetryPolicy{MaxAttempts: 1},
		TPS:        copyOpt.tps,
		DryRun:     true,
		Reporter:   progress.NewNop(),
		OnDecision: report.add,
	})
	if err != nil {
		if isCanceled(err) {
			return withExitCode(ExitInterrupted, fmt.Errorf("中断しました"))
		}
		return err
	}

	report.write(os.Stdout)

	// 差分があれば終了コードで知らせる。
	// バックアップの確認をジョブから行えるようにするため。
	if report.differing > 0 {
		return withExitCode(ExitTransferFailed,
			fmt.Errorf("%d件の差分があります", report.differing))
	}
	return nil
}

// checkReport は差分の一覧を組み立てます。
type checkReport struct {
	showAll bool

	mu        sync.Mutex
	rows      []checkRow
	differing int
	same      int
	bytes     int64
}

type checkRow struct {
	path   string
	size   int64
	reason string
}

func (r *checkReport) add(ev transfer.DecisionEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ev.Action == transfer.ActionCopy {
		r.differing++
		if ev.Size > 0 {
			r.bytes += ev.Size
		}
		r.rows = append(r.rows, checkRow{path: ev.Path, size: ev.Size, reason: ev.Reason})
		return
	}

	r.same++
	if r.showAll {
		r.rows = append(r.rows, checkRow{path: ev.Path, size: ev.Size, reason: ev.Reason})
	}
}

func (r *checkReport) write(w *os.File) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.rows) == 0 {
		fmt.Fprintln(w, "差分はありません。")
		return
	}

	// 並び順を安定させる。走査は並行しないが、
	// 表示の再現性があると比べやすい。
	sortRows(r.rows)

	tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
	for _, row := range r.rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", row.path, progress.HumanBytes(row.size), row.reason)
	}
	tw.Flush()

	fmt.Fprintf(w, "\n差分 %d件 / %s", r.differing, progress.HumanBytes(r.bytes))
	if r.same > 0 {
		fmt.Fprintf(w, "、一致 %d件", r.same)
	}
	fmt.Fprintln(w)
}

func sortRows(rows []checkRow) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].path < rows[j].path })
}
