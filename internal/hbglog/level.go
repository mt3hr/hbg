// Package hbglog は hbg のログ出力を扱います。
//
// 標準の log/slog に自前のハンドラを組み合わせ、$HOME/hbg/logs 配下の
// レベル別ファイルへ JSON で 1 行 1 レコードずつ書き出します。
// 外部のログライブラリには依存しません。
//
// この構成は同じ作者の gkill に合わせています。ただし gkill には
// ローテーションがなく、詳細なログを常用するとファイルが際限なく
// 肥大化するため、hbg ではサイズと世代でローテーションします。
package hbglog

import (
	"fmt"
	"log/slog"
	"strings"
)

// ログレベル。
//
// Transfer は「1ファイルの転送結果」を記録するための独自レベルです。
// 情報量としては Info より詳しく Debug より粗いという位置づけで、
// gkill の Access レベルに対応します。
const (
	LevelTrace    = slog.LevelDebug - 4
	LevelDebug    = slog.LevelDebug
	LevelTransfer = slog.LevelDebug + 2
	LevelInfo     = slog.LevelInfo
	LevelWarn     = slog.LevelWarn
	LevelError    = slog.LevelError

	// LevelNone はすべての出力を止めます。
	LevelNone = slog.LevelError + 100
)

// levelNames はレベルと名前の対応です。ファイル名にも使います。
var levelNames = []struct {
	level slog.Level
	name  string
}{
	{LevelTrace, "trace"},
	{LevelDebug, "debug"},
	{LevelTransfer, "transfer"},
	{LevelInfo, "info"},
	{LevelWarn, "warn"},
	{LevelError, "error"},
}

// LevelName はレベルの表示名を返します。
func LevelName(l slog.Level) string {
	for _, e := range levelNames {
		if e.level == l {
			return strings.ToUpper(e.name)
		}
	}
	return l.String()
}

// levelFileName はレベルに対応するログファイル名を返します。
func levelFileName(l slog.Level) string {
	for _, e := range levelNames {
		if e.level == l {
			return "hbg_" + e.name + ".log"
		}
	}
	return ""
}

// ParseLevel は名前からログレベルを求めます。
func ParseLevel(name string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "none", "off":
		return LevelNone, nil
	case "error":
		return LevelError, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "info":
		return LevelInfo, nil
	case "transfer":
		return LevelTransfer, nil
	case "debug":
		return LevelDebug, nil
	case "trace":
		return LevelTrace, nil
	}
	return 0, fmt.Errorf("不明なログレベル %q（none, error, warn, info, transfer, debug, trace のいずれか）", name)
}

// LevelNames は指定できるログレベルの名前を返します。
func LevelNames() []string {
	return []string{"none", "error", "warn", "info", "transfer", "debug", "trace"}
}
