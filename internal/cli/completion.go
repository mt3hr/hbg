package cli

import (
	"os"

	"github.com/spf13/cobra"
)

// completionCmd は入力補完の設定を書き出します。
//
// cobra が持っている仕組みをそのまま使います。
// 設定ファイルを読む必要はないので、読み込みを飛ばします。
var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "入力補完の設定を書き出す",
	Long: `シェルの入力補完の設定を書き出します。

bash:
  # 一度だけ試す
  source <(hbg completion bash)
  # 毎回使う
  hbg completion bash > /etc/bash_completion.d/hbg

zsh:
  hbg completion zsh > "${fpath[1]}/_hbg"

fish:
  hbg completion fish > ~/.config/fish/completions/hbg.fish

PowerShell:
  hbg completion powershell | Out-String | Invoke-Expression
  # 毎回使うにはプロファイルに追記してください
  hbg completion powershell >> $PROFILE
`,
	Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
	Annotations: map[string]string{
		skipConfigLoadAnnotation: "true",
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return cmd.Root().GenBashCompletionV2(os.Stdout, true)
		case "zsh":
			return cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			return cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
		}
		return nil
	},
}
