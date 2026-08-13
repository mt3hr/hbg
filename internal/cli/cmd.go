package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/mt3hr/hbg"
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
	err := rootCmd.Execute()
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
	Dropbox       []DropboxConfig
	GoogleDrive   []GoogleDriveConfig
	Local         struct {
		Name string
	}
}

// DropboxConfig は設定ファイルの Dropbox 1件ぶんです。
type DropboxConfig struct {
	Name string
	// AppKey は Dropbox アプリのキーです。
	// 省略時は環境変数 HBG_DROPBOX_APP_KEY やビルド時の埋め込み値が使われます。
	AppKey string `mapstructure:"app_key"`
	// AccessToken を指定すると OAuth の認可を行わずこのトークンを使います。
	// 設定ファイルへの直接記述は避け、${環境変数} での指定を推奨します。
	//
	// 以前は Token というフィールドがありましたが、宣言されているだけで
	// どこからも読まれていませんでした。後方互換のため引き続き受け付けます。
	AccessToken string `mapstructure:"access_token"`
	Token       string `mapstructure:"token"`
}

func (c DropboxConfig) toStorageConfig() hbg.DropboxConfig {
	token := c.AccessToken
	if token == "" {
		token = c.Token
	}
	return hbg.DropboxConfig{
		Name:        c.Name,
		AppKey:      os.ExpandEnv(c.AppKey),
		AccessToken: os.ExpandEnv(token),
	}
}

// GoogleDriveConfig は設定ファイルの Google Drive 1件ぶんです。
type GoogleDriveConfig struct {
	Name string
	// ClientID と ClientSecret は OAuth クライアントの識別情報です。
	// 省略時は環境変数 HBG_GOOGLE_CLIENT_ID / HBG_GOOGLE_CLIENT_SECRET や
	// ビルド時の埋め込み値が使われます。
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
}

func (c GoogleDriveConfig) toStorageConfig() hbg.GoogleDriveConfig {
	return hbg.GoogleDriveConfig{
		Name:         c.Name,
		ClientID:     os.ExpandEnv(c.ClientID),
		ClientSecret: os.ExpandEnv(c.ClientSecret),
	}
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

ストレージは設定ファイル hbg_config.yaml で名前を付けて定義し、
コマンドでは "名前:パス" の形式で指定します。`,
		SilenceUsage:  true,
		SilenceErrors: true, // エラーの表示は Execute で行う
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
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
	rootCmd.AddCommand(removeCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(shellCmd)
	rootCmd.AddCommand(configCmd)
	rootCmd.AddCommand(authCmd)

	rootPf := rootCmd.PersistentFlags()
	rootPf.StringVar(&rootOpt.configfile, "config_file", "", "設定ファイルのパス")
}

func storageMapFromConfig(c *Config) (map[string]hbg.Storage, error) {
	storages := map[string]hbg.Storage{}

	// localの読み込み
	storages[c.Local.Name] = hbg.NewLocalFileSystem(c.Local.Name)

	// dropboxの読み込み
	for _, dbxCfg := range c.Dropbox {
		if _, exist := storages[dbxCfg.Name]; exist {
			return nil, fmt.Errorf("ストレージ名が重複しています: '%s'", dbxCfg.Name)
		}
		dropbox, err := hbg.NewDropbox(dbxCfg.toStorageConfig())
		if err != nil {
			return nil, err
		}
		storages[dbxCfg.Name] = dropbox
	}

	// googledriveの読み込み
	for _, gdvCfg := range c.GoogleDrive {
		if _, exist := storages[gdvCfg.Name]; exist {
			return nil, fmt.Errorf("ストレージ名が重複しています: '%s'", gdvCfg.Name)
		}
		googleDrive, err := hbg.NewGoogleDrive(gdvCfg.toStorageConfig())
		if err != nil {
			return nil, err
		}
		storages[gdvCfg.Name] = googleDrive
	}
	return storages, nil
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
