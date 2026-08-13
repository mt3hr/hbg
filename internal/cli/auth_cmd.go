package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/mt3hr/hbg/backend/dropbox"
	"github.com/mt3hr/hbg/backend/googledrive"
	"github.com/mt3hr/hbg/internal/auth"
	"github.com/spf13/cobra"
)

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "クラウドストレージの認証を行う",
	Long: `Dropbox や Google Drive の認証を行います。

認証はブラウザで行います。hbg が一時的にローカルの待ち受けを開き、
許可のあとリダイレクトされてくる認可コードを受け取ります。

取得したトークンは $HOME/hbg/tokens に保存され、期限が切れても
自動的に更新されます。`,
}

var authOpt = struct {
	noBrowser bool
}{}

var authLoginCmd = &cobra.Command{
	Use:   "login <ストレージ名>",
	Short: "認証してトークンを保存する",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]

		// 認証中の Ctrl-C で待ち受けを畳めるようにする。
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()

		opts := auth.LoginOptions{
			OpenBrowser: !authOpt.noBrowser,
			Prompt: func(authURL string) {
				fmt.Println("ブラウザで次のURLを開き、hbg のアクセスを許可してください。")
				if !authOpt.noBrowser {
					fmt.Println("（自動で開かない場合は手でコピーしてください）")
				}
				fmt.Printf("\n%s\n\n", authURL)
				fmt.Println("許可が終わるまで待っています...")
			},
		}

		if dbxCfg, ok := findDropboxConfig(name); ok {
			if err := dropbox.Login(ctx, dbxCfg.toStorageConfig(), opts); err != nil {
				return authLoginError(name, err)
			}
			reportLoginSuccess(dropbox.Type, name)
			return nil
		}

		if gdvCfg, ok := findGoogleDriveConfig(name); ok {
			if err := googledrive.Login(ctx, gdvCfg.toStorageConfig(), opts); err != nil {
				return authLoginError(name, err)
			}
			reportLoginSuccess(googledrive.Type, name)
			return nil
		}

		return withExitCode(ExitUsage, fmt.Errorf(
			"設定に認証が必要なストレージ %q がありません。%s を確認してください",
			name, mustConfigFile()))
	},
}

func authLoginError(name string, err error) error {
	if errors.Is(err, auth.ErrDenied) {
		return fmt.Errorf("%s の認証は許可されませんでした", name)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s の認証を中断しました", name)
	}
	return err
}

func reportLoginSuccess(storageType, name string) {
	store := auth.NewFileStore()
	fmt.Printf("\n%s の認証が完了しました。\n", name)
	fmt.Printf("トークンの保存先: %s\n", store.Path(storageType, name))
}

var authLogoutCmd = &cobra.Command{
	Use:   "logout <ストレージ名>",
	Short: "保存されたトークンを削除する",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		name := args[0]
		store := auth.NewFileStore()

		types := configuredStorageTypes(name)
		if len(types) == 0 {
			return withExitCode(ExitUsage, fmt.Errorf("設定にストレージ %q がありません", name))
		}

		// 同じ名前が複数の種別に設定されていることは通常ないが、
		// あった場合はすべて削除する。
		for _, storageType := range types {
			if err := store.Delete(storageType, name); err != nil {
				return err
			}
		}
		fmt.Printf("%s のトークンを削除しました。\n", name)
		return nil
	},
}

var authStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "認証の状態を表示する",
	Args:  cobra.NoArgs,
	RunE: func(_ *cobra.Command, _ []string) error {
		store := auth.NewFileStore()

		type row struct {
			name        string
			storageType string
		}
		rows := []row{}
		for _, c := range config.Dropbox {
			rows = append(rows, row{c.Name, dropbox.Type})
		}
		for _, c := range config.GoogleDrive {
			rows = append(rows, row{c.Name, googledrive.Type})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

		if len(rows) == 0 {
			fmt.Println("認証が必要なストレージは設定されていません。")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		fmt.Fprintln(w, "名前\t種別\t状態\t保存先")
		for _, r := range rows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				r.name, r.storageType, authStatusOf(store, r.storageType, r.name),
				store.Path(r.storageType, r.name))
		}
		return w.Flush()
	},
}

// authStatusOf は保存されたトークンの状態を人間向けの文字列にします。
func authStatusOf(store auth.Store, storageType, name string) string {
	tok, err := store.Load(storageType, name)
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			return "未認証"
		}
		return "読み取り失敗"
	}

	if tok.RefreshToken == "" {
		// 更新できないトークンは失効したら手動で取り直すことになる。
		return "認証済み（更新不可・要再認証）"
	}
	if tok.Expiry.IsZero() {
		return "認証済み"
	}
	if time.Now().After(tok.Expiry) {
		return "認証済み（期限切れ・自動更新されます）"
	}
	return fmt.Sprintf("認証済み（%s まで有効）", tok.Expiry.Local().Format("2006-01-02 15:04"))
}

func findDropboxConfig(name string) (DropboxConfig, bool) {
	for _, c := range config.Dropbox {
		if c.Name == name {
			return c, true
		}
	}
	return DropboxConfig{}, false
}

func findGoogleDriveConfig(name string) (GoogleDriveConfig, bool) {
	for _, c := range config.GoogleDrive {
		if c.Name == name {
			return c, true
		}
	}
	return GoogleDriveConfig{}, false
}

// configuredStorageTypes は、その名前が設定されているストレージ種別を返します。
func configuredStorageTypes(name string) []string {
	types := []string{}
	if _, ok := findDropboxConfig(name); ok {
		types = append(types, dropbox.Type)
	}
	if _, ok := findGoogleDriveConfig(name); ok {
		types = append(types, googledrive.Type)
	}
	return types
}

func init() {
	authCmd.AddCommand(authLoginCmd)
	authCmd.AddCommand(authLogoutCmd)
	authCmd.AddCommand(authStatusCmd)

	authLoginCmd.Flags().BoolVar(&authOpt.noBrowser, "no-browser", false,
		"ブラウザを自動で開かない（URLを手で開く）")
}
