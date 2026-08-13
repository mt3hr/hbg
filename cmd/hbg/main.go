// Command hbg は、ローカルとクラウドストレージの間でファイルをコピー・同期するCLIです。
//
// インストール:
//
//	go install github.com/mt3hr/hbg/cmd/hbg@latest
package main

import "github.com/mt3hr/hbg/internal/cli"

func main() {
	cli.Execute()
}
