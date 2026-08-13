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
	// 設定ファイルを作る・場所を見るコマンドなので、
	// 設定ファイルがまだ無くても動く必要がある。
	Annotations: map[string]string{skipConfigLoadAnnotation: "true"},
	Long: `設定ファイルの作成・場所の確認を行います。

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
	configCmd.AddCommand(configPathCmd)
}

// warnf は警告を標準エラー出力に書きます。
// 標準出力はコマンドの結果を流すために空けておきます。
func warnf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "hbg: 警告: "+format+"\n", a...)
}
