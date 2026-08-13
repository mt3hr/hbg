package cli

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/mt3hr/hbg/internal/hbghome"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "設定ファイルを操作する",
	// 設定ファイルを作る・移す・場所を見るコマンドなので、
	// 設定ファイルがまだ無くても動く必要がある。
	Annotations: map[string]string{skipConfigLoadAnnotation: "true"},
	Long: `設定ファイルの作成・移行・場所の確認を行います。

hbg は設定・認証情報・ログ・キャッシュをすべて 1 つのディレクトリに
まとめて保存します。既定は $HOME/hbg で、環境変数 HBG_HOME で変更できます。`,
}

var configInitCmd = &cobra.Command{
	Use:   "init",
	Short: "設定ファイルの雛形を作成する",
	Args:  cobra.NoArgs,
	RunE: func(_ *cobra.Command, _ []string) error {
		path, err := hbghome.ConfigFile()
		if err != nil {
			return err
		}

		if _, err := os.Stat(path); err == nil {
			return withExitCode(ExitUsage,
				fmt.Errorf("設定ファイルはすでにあります: %s", path))
		}

		if err := hbghome.WriteSecretFile(path, []byte(defaultConfigYAML())); err != nil {
			return err
		}
		fmt.Printf("設定ファイルを作成しました: %s\n", path)
		fmt.Println("使うストレージに合わせて編集してください。")
		return nil
	},
}

var configMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "旧レイアウトの設定・認証情報を移行する",
	Long: `ホームディレクトリ直下や一時ディレクトリに散らばっていた
設定ファイル・トークン・シェル履歴を $HOME/hbg 配下へ移します。

移動元のファイルは削除せず、末尾に .migrated を付けて残します。
移動先にすでにファイルがある場合は上書きしません。`,
	Args: cobra.NoArgs,
	RunE: func(_ *cobra.Command, _ []string) error {
		pending, err := hbghome.PendingMigrations()
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			fmt.Println("移行するものはありません。")
			return nil
		}

		// 移行先を先に見せる。HBG_HOME を別の場所へ向けたまま実行すると
		// 意図しない場所へ集約されてしまうため。
		root, err := hbghome.Root()
		if err != nil {
			return err
		}
		fmt.Printf("移行先: %s\n", root)
		if os.Getenv(hbghome.EnvHome) != "" {
			fmt.Printf("（%s が設定されています）\n", hbghome.EnvHome)
		}
		fmt.Println()

		done, err := hbghome.Migrate()
		for _, m := range done {
			if m.Skipped {
				fmt.Printf("スキップ: %s\n", m)
				continue
			}
			fmt.Printf("移動: %s\n", m)
		}
		if err != nil {
			return err
		}
		fmt.Println("\n移動元のファイルは .migrated を付けて残しています。")
		return nil
	},
}

var configPathCmd = &cobra.Command{
	Use:   "path",
	Short: "設定・認証情報・ログの場所を表示する",
	Args:  cobra.NoArgs,
	RunE: func(_ *cobra.Command, _ []string) error {
		type entry struct {
			label string
			get   func() (string, error)
		}
		entries := []entry{
			{"ルート", hbghome.Root},
			{"設定ファイル", hbghome.ConfigFile},
			{"トークン", hbghome.TokensDir},
			{"資格情報", hbghome.CredentialsDir},
			{"ログ", hbghome.LogsDir},
			{"キャッシュ", hbghome.CachesDir},
			{"シェル履歴", hbghome.ShellHistoryFile},
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		for _, e := range entries {
			path, err := e.get()
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "%s\t%s\t%s\n", e.label, path, existenceMark(path))
		}
		if err := w.Flush(); err != nil {
			return err
		}

		if loadedConfigFile != "" && loadedConfigFile != mustConfigFile() {
			fmt.Printf("\n実際に読み込んだ設定ファイル: %s\n", loadedConfigFile)
			fmt.Println("（非推奨の場所です。hbg config migrate で移せます）")
		}

		pending, err := hbghome.PendingMigrations()
		if err == nil && len(pending) > 0 {
			fmt.Printf("\n未移行のファイルが %d件あります。hbg config migrate で移せます:\n", len(pending))
			for _, m := range pending {
				fmt.Printf("  %s\n", m.From)
			}
		}

		return nil
	},
}

// existenceMark はパスの存在有無を表す短い印を返します。
func existenceMark(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "(なし)"
	}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil || len(entries) == 0 {
			return "(空)"
		}
		return fmt.Sprintf("(%d件)", len(entries))
	}
	return ""
}

func init() {
	configCmd.AddCommand(configInitCmd)
	configCmd.AddCommand(configMigrateCmd)
	configCmd.AddCommand(configPathCmd)
}

// warnf は警告を標準エラー出力に書きます。
// 標準出力はコマンドの結果を流すために空けておきます。
func warnf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "hbg: 警告: "+format+"\n", a...)
}
