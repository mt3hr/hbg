package cli

import (
	"strings"

	"github.com/mt3hr/hbg/backend"
)

// supportedStorageTypesHelp は、対応しているストレージの一覧を返します。
//
// 一覧は登録されているバックエンドから作ります。
// 以前はヘルプの文字列に種別が直接書かれており、実装が削除されたあとも
// ftp が案内され続けていました。
func supportedStorageTypesHelp() string {
	var sb strings.Builder
	sb.WriteString("対応しているストレージ:\n")

	for _, d := range backend.Descriptors() {
		if d.Type == "memory" {
			continue // テスト用なので案内しない
		}
		sb.WriteString("  ")
		sb.WriteString(d.Type)
		if d.Summary != "" {
			sb.WriteString(strings.Repeat(" ", max(1, 14-len(d.Type))))
			sb.WriteString(d.Summary)
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// defaultConfigFromDescriptors は、登録されているバックエンドから
// 設定ファイルの雛形を組み立てます。
func defaultConfigFromDescriptors() string {
	var sb strings.Builder
	sb.WriteString("storages:\n")

	for _, d := range backend.Descriptors() {
		if d.ConfigDoc == "" || d.Type == "memory" {
			continue
		}
		sb.WriteString(d.ConfigDoc)
		sb.WriteString("\n")
	}
	return sb.String()
}
