package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mt3hr/hbg/storage"
	"github.com/spf13/cobra"
)

var (
	listCmd = &cobra.Command{
		Aliases: []string{"ls"},
		RunE:    runList,
		Args:    cobra.ExactArgs(1),
		Use:     "list storage:path",
		Short:   "ストレージのファイルを一覧表示する",
		PreRunE: func(_ *cobra.Command, args []string) error {
			targetInfo := args[0]
			targetSplit := strings.SplitN(targetInfo, ":", 2)

			if len(targetSplit) < 2 {
				return withExitCode(ExitUsage, fmt.Errorf("pathの記述が変です: %q（storage:path の形式で指定してください）", targetInfo))
			}
			listOpt.targetStorage = targetSplit[0]
			listOpt.targetPath = targetSplit[1]
			return nil
		},
	}
	listOpt = &struct {
		targetStorage string
		targetPath    string
		long          bool
		humanReadable bool
	}{}
)

func init() {
	fs := listCmd.Flags()
	fs.BoolVarP(&listOpt.long, "long", "l", false, "")
	fs.BoolVarP(&listOpt.humanReadable, "human-readable", "r", false, "")
}

const (
	_ = iota
	// KB .
	KB int64 = 1 << (10 * iota)
	// MB .
	MB
	// GB .
	GB
	// TB .
	TB
)

// humanReadableSize は、バイト数を人間が読みやすい単位の文字列にします。
// 単位は1024進で、小数第1位まで切り捨てて表示します（例: 1536 -> "1.5K"）。
//
// 旧実装は小数部を求める際に「KBの個数」で剰余を取っており
// （b = (((size % TB) % GB) % MB) % kb）、表示される小数が実際の値と
// 対応していませんでした。ここで正しい計算に修正しています。
func humanReadableSize(size int64) string {
	if size == 0 {
		return "0B"
	}

	negative := size < 0
	if negative {
		size = -size
	}

	var s string
	switch {
	case size >= TB:
		s = formatSizeWithUnit(size, TB, "T")
	case size >= GB:
		s = formatSizeWithUnit(size, GB, "G")
	case size >= MB:
		s = formatSizeWithUnit(size, MB, "M")
	case size >= KB:
		s = formatSizeWithUnit(size, KB, "K")
	default:
		s = strconv.FormatInt(size, 10) + "B"
	}

	if negative {
		return "-" + s
	}
	return s
}

// formatSizeWithUnit は size を unit で割り、小数第1位まで表示した文字列を返します。
// 小数部は切り捨てです。浮動小数点を使わないため丸め誤差が出ません。
func formatSizeWithUnit(size, unit int64, suffix string) string {
	whole := size / unit
	frac := (size % unit) * 10 / unit
	return strconv.FormatInt(whole, 10) + "." + strconv.FormatInt(frac, 10) + suffix
}

func runList(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	// ここで初めて、対象のストレージだけが組み立てられる。
	s, err := resolver.Get(ctx, listOpt.targetStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	return list(ctx, s, listOpt.targetPath, listOpt.long, listOpt.humanReadable)
}

func list(ctx context.Context, s storage.Storage, path string, long, humanReadable bool) error {
	entries, err := storage.ListAllSorted(ctx, s, path)
	if err != nil {
		return fmt.Errorf("error at list at %s. %w", path, err)
	}

	w := &tabwriter.Writer{}
	w.Init(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	for _, file := range entries {
		isDir := ""
		timestr := ""
		sizestr := ""
		if file.IsDir {
			isDir = "dir"
		} else {
			isDir = "file"
			timestr = file.ModTime.Format(time.RFC3339)

			if humanReadable {
				sizestr = humanReadableSize(file.Size)
			} else {
				sizestr = strconv.FormatInt(file.Size, 10)
			}
		}

		fmt.Fprintf(w, "%s", file.Name)
		if long {
			fmt.Fprintf(w, "\t%s\t%s\t%s", isDir, timestr, sizestr)
		}
		fmt.Fprintf(w, "\n")
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("error at write list output. %w", err)
	}
	return nil
}
