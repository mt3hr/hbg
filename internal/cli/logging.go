package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/mt3hr/hbg/internal/hbglog"
	"github.com/spf13/cobra"
)

// Version は hbg のバージョンです。
// ビルド時に -ldflags で埋め込みます。
var Version = "dev"

var logOpt = struct {
	level      string
	stdout     bool
	maxSizeMiB int64
	maxBackups int
	maxAgeDays int
}{
	// 既定は info。
	//
	// gkill は既定が none（無出力）ですが、hbg では「あとから
	// どのファイルが失敗したか追える」ことが目的なので、
	// 既定で記録されないと意味がありません。
	level:      "info",
	maxSizeMiB: 10,
	maxBackups: 5,
	maxAgeDays: 30,
}

// initLogging はログを初期化します。
func initLogging() error {
	level, err := hbglog.ParseLevel(logOpt.level)
	if err != nil {
		return err
	}

	_, err = hbglog.Init(hbglog.Options{
		MinLevel:     level,
		Mode:         hbglog.ModeMergedAndSplit,
		MaxSizeBytes: logOpt.maxSizeMiB * 1024 * 1024,
		MaxBackups:   logOpt.maxBackups,
		MaxAge:       time.Duration(logOpt.maxAgeDays) * 24 * time.Hour,
		Stdout:       logOpt.stdout,
		Version:      Version,
	})
	return err
}

// closeLogging はログファイルを閉じます。
func closeLogging() {
	_ = hbglog.Close()
}

func init() {
	pf := rootCmd.PersistentFlags()
	pf.StringVar(&logOpt.level, "log", logOpt.level,
		"ログレベル ("+strings.Join(hbglog.LevelNames(), ", ")+")")
	pf.BoolVar(&logOpt.stdout, "log-stdout", false, "ログを標準出力にも書く")
	pf.Int64Var(&logOpt.maxSizeMiB, "log-max-size", logOpt.maxSizeMiB,
		"ログ1ファイルの上限 (MiB)")
	pf.IntVar(&logOpt.maxBackups, "log-max-backups", logOpt.maxBackups,
		"保持するログの世代数")
	pf.IntVar(&logOpt.maxAgeDays, "log-max-age", logOpt.maxAgeDays,
		"ログを保持する日数 (0 で削除しない)")

	versionCmd := &cobra.Command{
		Use:         "version",
		Short:       "バージョンを表示する",
		Args:        cobra.NoArgs,
		Annotations: map[string]string{skipConfigLoadAnnotation: "true"},
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("hbg", Version)
		},
	}
	rootCmd.AddCommand(versionCmd)
}
