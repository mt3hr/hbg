package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync srcStorage:srcPath destStorage:destDirPath",
	Short: "コピー先をコピー元に合わせる",
	Long: `コピー元の内容をコピー先に反映します。

--delete を付けると、コピー元にないものをコピー先から削除します。
付けない場合の動きは copy と同じです。

削除には次の決まりがあります。取り返しがつかない操作なので、
判断に迷いのある場面では消しません。

  - 転送に1件でも失敗があれば、削除は行いません。
    コピー元を読めなかっただけで「向こうには無い」と判断すると、
    取っておきたいものを消すことになるためです。

  - --include や --exclude で対象外にしたものは消しません。
    転送していないものを消すのは筋が通らないためです。

--dry-run を付けると、何が消えるかだけを確かめられます。
はじめて実行するときは、まずこちらで確かめてください。

` + supportedStorageTypesHelp(),
	Example: `使用例
hbg sync local:C:/photos dropbox:/backup
hbg sync --delete --dry-run local:C:/photos dropbox:/backup
hbg sync --delete local:C:/photos dropbox:/backup
`,
	Args:    cobra.ExactArgs(2),
	PreRunE: copyCmd.PreRunE,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runTransfer(cmd, syncOpt.delete)
	},
}

var syncOpt = struct {
	delete bool
}{}

func init() {
	fs := syncCmd.Flags()
	registerTransferFlags(fs)
	fs.BoolVar(&syncOpt.delete, "delete", false,
		"コピー元にないものをコピー先から削除する")
}

// deleteSummary は削除の結果を1行にまとめます。
func deleteSummary(deleted, failed int) string {
	if deleted == 0 && failed == 0 {
		return ""
	}
	if failed == 0 {
		return fmt.Sprintf("削除: %d件", deleted)
	}
	return fmt.Sprintf("削除: %d件成功, %d件失敗", deleted, failed)
}
