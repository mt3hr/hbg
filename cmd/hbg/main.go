// Command hbg は、ローカルとクラウドストレージの間でファイルをコピー・同期するCLIです。
//
// インストール:
//
//	go install github.com/mt3hr/hbg/cmd/hbg@latest
package main

import (
	"os"

	"github.com/mt3hr/hbg/internal/cli"
)

func main() {
	// 終了コードは cli.Execute が決める。
	// 一部のファイルの転送に失敗した場合は 3 を返すため、
	// スクリプトから成否を判定できる。
	os.Exit(cli.Execute())
}
