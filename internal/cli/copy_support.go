package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/mt3hr/hbg/progress"
	"github.com/mt3hr/hbg/transfer"
)

// newReporter は進みぐあいの表示先を作ります。
//
// 端末なら進捗バー、パイプやジョブなら1行ずつの表示になります。
func newReporter() (progress.Reporter, error) {
	mode, ok := progress.ParseMode(copyOpt.progress)
	if !ok {
		return nil, fmt.Errorf("--progress の指定が不正です: %q（%s のいずれか）",
			copyOpt.progress, strings.Join(progress.ModeNames(), ", "))
	}
	if copyOpt.quiet {
		mode = progress.ModeNone
	}

	return progress.New(progress.Options{
		Mode:          mode,
		MaxBars:       copyOpt.progressBars,
		StatsInterval: copyOpt.stats,
	}), nil
}

// writeSummary は結果の要約を書き出します。
func writeSummary(w io.Writer, r *transfer.Result) {
	fmt.Fprintf(w, "\nコピー完了: %d件成功", r.Transferred)
	if r.Skipped > 0 {
		fmt.Fprintf(w, ", %d件スキップ", r.Skipped)
	}
	fmt.Fprintf(w, ", %d件失敗", r.Failed)
	if r.Elapsed > 0 {
		fmt.Fprintf(w, " (%s)", r.Elapsed.Round(time.Millisecond))
	}
	fmt.Fprintln(w)

	if s := deleteSummary(r.Deleted, r.DeleteFailed); s != "" {
		fmt.Fprintln(w, s)
	}

	if len(r.Errors) == 0 {
		return
	}

	fmt.Fprintf(w, "\n失敗した内容:\n")
	for _, err := range r.Errors {
		fmt.Fprintf(w, "  %v\n", err)
	}
	if r.Failed > len(r.Errors) {
		fmt.Fprintf(w, "  ... ほか %d件\n", r.Failed-len(r.Errors))
	}
	if r.Aborted {
		fmt.Fprintln(w, "\n失敗が多かったため中断しました。")
	}
}

// isCanceled はエラーが取り消しを表すかを返します。
func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// parseByteSize は "10M" のような指定をバイト数に変換します。
// 空文字なら 0（無制限）を返します。
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	multiplier := int64(1)
	switch last := s[len(s)-1]; last {
	case 'k', 'K':
		multiplier = 1 << 10
		s = s[:len(s)-1]
	case 'm', 'M':
		multiplier = 1 << 20
		s = s[:len(s)-1]
	case 'g', 'G':
		multiplier = 1 << 30
		s = s[:len(s)-1]
	case 't', 'T':
		multiplier = 1 << 40
		s = s[:len(s)-1]
	}

	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("数値として読めません: %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("負の値は指定できません: %q", s)
	}
	return int64(n * float64(multiplier)), nil
}

// buildComparePolicy はフラグから比較の規則を組み立てます。
func buildComparePolicy() (transfer.ComparePolicy, error) {
	spec := copyOpt.compare
	switch {
	case copyOpt.checksum && copyOpt.sizeOnly:
		return transfer.ComparePolicy{}, fmt.Errorf("--checksum と --size-only は同時に指定できません")
	case copyOpt.checksum:
		spec = "size,hash"
	case copyOpt.sizeOnly:
		spec = "size"
	}

	fields, err := transfer.ParseCompareFields(spec)
	if err != nil {
		return transfer.ComparePolicy{}, fmt.Errorf("--compare の指定が不正です: %w", err)
	}

	// 古い名前が使われていればそちらを尊重する。
	window := copyOpt.modifyWindow
	if window == 0 {
		window = copyOpt.updateDuration
	}

	return transfer.ComparePolicy{
		Fields:         fields,
		ModifyWindow:   window,
		Update:         copyOpt.update && !copyOpt.overwrite,
		IgnoreExisting: copyOpt.ignoreExisting,
	}, nil
}

// buildFilter はフラグから絞り込みを組み立てます。
func buildFilter() (*transfer.Filter, error) {
	minSize, err := parseByteSize(copyOpt.minSize)
	if err != nil {
		return nil, fmt.Errorf("--min-size の指定が不正です: %w", err)
	}
	maxSize, err := parseByteSize(copyOpt.maxSize)
	if err != nil {
		return nil, fmt.Errorf("--max-size の指定が不正です: %w", err)
	}

	return transfer.NewFilter(transfer.FilterSpec{
		Ignore:  copyOpt.ignore,
		Include: copyOpt.include,
		Exclude: copyOpt.exclude,
		MinSize: minSize,
		MaxSize: maxSize,
	})
}
