//go:build !windows

package local

// clearReadOnly は Windows 以外では何もしません。
//
// 読み取り専用属性という概念が無く、rename が通るかどうかは
// 置き換え先のパーミッションではなく親ディレクトリの書き込み権で決まるためです。
// ここでパーミッションを触ると、利用者が意図して付けた権限を
// 書き換えてしまいます。
func clearReadOnly(string) error { return nil }
