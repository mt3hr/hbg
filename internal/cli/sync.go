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
    件数が多く、失敗が避けられない場面で削除を進めたいときは
    --delete-on-partial を付けてください。付けても、一覧に失敗した
    ディレクトリの中身は削除対象になりません。

  - --include や --exclude で対象外にしたものは消しません。
    転送していないものを消すのは筋が通らないためです。
    例外は hbg 自身が置き去りにした書き込み中ファイル(.hbgpart)で、
    これは利用者のデータではないので絞り込みに関わらず片付けます。

--dry-run を付けると、何が消えるかだけを確かめられます。
はじめて実行するときは、まずこちらで確かめてください。

コピー先は「コピー先ディレクトリ / コピー元の名前」になります。
local:/data/photos を local:/backup へ同期すると /backup/photos が
コピー元に合わせられ、/backup の他のものには手を付けません。

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
	delete          bool
	deleteOnPartial bool
}{}

func init() {
	fs := syncCmd.Flags()
	registerTransferFlags(fs)
	fs.BoolVar(&syncOpt.delete, "delete", false,
		"コピー元にないものをコピー先から削除する")
	fs.BoolVar(&syncOpt.deleteOnPartial, "delete-on-partial", false,
		"転送に失敗があっても削除する（--delete と併用）")
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
