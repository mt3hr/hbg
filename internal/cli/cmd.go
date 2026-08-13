package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mt3hr/hbg/backend"
	_ "github.com/mt3hr/hbg/backend/ftp"      // 種別 ftp を登録する
	_ "github.com/mt3hr/hbg/backend/local"    // 種別 local を登録する
	_ "github.com/mt3hr/hbg/backend/onedrive" // 種別 onedrive を登録する
	_ "github.com/mt3hr/hbg/backend/s3"       // 種別 s3 を登録する
	_ "github.com/mt3hr/hbg/backend/sftp"     // 種別 sftp を登録する
	_ "github.com/mt3hr/hbg/backend/smb"      // 種別 smb を登録する
	_ "github.com/mt3hr/hbg/backend/webdav"   // 種別 webdav を登録する
	"github.com/spf13/cobra"
)

// プロセスの終了コード。
//
// 以前は転送に失敗しても常に 0 で終了していたため、
// スクリプトやジョブから成否を判定できなかった。
const (
	// ExitOK は全件成功したことを表します。0件だった場合も成功です。
	ExitOK = 0
	// ExitFailure は設定の読み込み失敗など、実行そのものの失敗を表します。
	ExitFailure = 1
	// ExitUsage は引数や設定の記述が誤っていることを表します。
	ExitUsage = 2
	// ExitTransferFailed は一部のファイルの転送に失敗したことを表します。
	ExitTransferFailed = 3
	// ExitInterrupted は利用者が中断したことを表します。
	// Unix の慣習に合わせ、SIGINT による終了は 130 とします。
	ExitInterrupted = 130
)

// exitError は、特定の終了コードで終了したいことを表すエラーです。
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

// withExitCode はエラーに終了コードを結びつけます。
func withExitCode(code int, err error) error {
	if err == nil {
		return nil
	}
	return &exitError{code: code, err: err}
}

// Execute .
// コマンドを実行し、プロセスの終了コードを返します。
// main関数はこの戻り値で os.Exit します。
func Execute() int {
	defer closeLogging()

	// Ctrl-C で安全に中断できるようにする。
	//
	// 以前は context を扱っていなかったため、割り込みは書き込みの途中で
	// プロセスを殺すしかなく、中身の欠けたファイルが残っていた。
	// 取り消しを伝えることで、書き込み中の一時ファイルが片付けられる。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2度目の割り込みは即座に終了する。
	// 後始末に時間がかかっている場合に待たされないようにするため。
	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt)
		<-ch
		<-ch
		fmt.Fprintln(os.Stderr, "\nhbg: 強制終了します")
		os.Exit(ExitInterrupted)
	}()

	err := rootCmd.ExecuteContext(ctx)
	if err == nil {
		return ExitOK
	}

	fmt.Fprintf(os.Stderr, "hbg: %v\n", err)

	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code
	}
	return ExitFailure
}

// defaultWorker は、設定ファイルにも -w にも同時処理数の指定がない場合に使う値です。
// createDefaultConfigYAML が生成する設定と同じ値にしています。
const defaultWorker = 2

// Config .
// コンフィグファイルのデータモデル
type Config struct {
	DefaultWorker int

	// Storages は種別を type で指定して並べたストレージの一覧です。
	Storages []StorageEntry
}

var (
	rootCmd = &cobra.Command{
		Use:   "hbg",
		Short: "ローカルとクラウドストレージの間でファイルをコピー・同期する",
		Long: `hbg はローカルファイルシステムとクラウドストレージの間で
ファイルをコピー・同期するためのコマンドラインツールです。

対応しているストレージ:
  local        ローカルファイルシステム
  dropbox      Dropbox
  googledrive  Google Drive

ストレージは設定ファイル（$HOME/hbg/configs/config.yaml）で名前を付けて
定義し、コマンドでは "名前:パス" の形式で指定します。`,
		SilenceUsage:  true,
		SilenceErrors: true, // エラーの表示は Execute で行う
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if err := initLogging(); err != nil {
				// ログを開けなくても本来の処理は続けられる。
				warnf("ログを初期化できませんでした: %v", err)
			}

			// 設定ファイル自体を操作するコマンドは、設定がなくても動く必要がある。
			if skipsConfigLoad(cmd) {
				return nil
			}
			if err := loadConfig(); err != nil {
				return withExitCode(ExitUsage, err)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, _ []string) {
			// サブコマンドなしで実行されたらヘルプを出す
			_ = cmd.Help()
		},
	}

	rootOpt = &struct {
		configfile string
	}{}

	config = &Config{}
)

func init() {
	rootCmd.AddCommand(copyCmd)
	rootCmd.AddCommand(syncCmd)
	rootCmd.AddCommand(moveCmd)
	rootCmd.AddCommand(mkdirCmd)
	rootCmd.AddCommand(removeCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(shellCmd)
	rootCmd.AddCommand(configCmd)
	rootCmd.AddCommand(authCmd)
	rootCmd.AddCommand(checkCmd)
	rootCmd.AddCommand(completionCmd)

	rootPf := rootCmd.PersistentFlags()
	rootPf.StringVar(&rootOpt.configfile, "config_file", "", "設定ファイルのパス")
}

// resolverFromConfig は設定からストレージの解決器を作ります。
//
// ここではストレージを組み立てません。名前の重複や知らない種別は
// 検出しますが、接続や認証は実際に使われるまで行いません。
//
// 以前はどのコマンドでも設定にあるすべてのストレージを構築しており、
// ローカルのファイルを一覧するだけでクラウドの認証が走っていました。
func resolverFromConfig(c *Config) (*backend.Resolver, error) {
	entries, err := storageEntries(c)
	if err != nil {
		return nil, err
	}
	return backend.NewResolver(entries)
}

// skipConfigLoadAnnotation が付いたコマンドは、設定ファイルを読み込まずに実行します。
const skipConfigLoadAnnotation = "hbg/skip-config-load"

// skipsConfigLoad は、そのコマンド（または祖先）が設定ファイルの
// 読み込みを必要としないかを返します。
func skipsConfigLoad(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		if c.Annotations[skipConfigLoadAnnotation] == "true" {
			return true
		}
	}
	return false
}

func getConfigFile() string {
	return rootOpt.configfile
}

func getConfig() *Config {
	return config
}
