package dropbox

import (
	"path"
	"strings"
)

// Dropbox のパスの決まり:
//
//   - 区切りは "/" で、必ず "/" で始まる。
//   - ルートだけは空文字で表す。"/" を渡すと API がエラーを返す。
//   - 大文字小文字は区別されないが、表示用のパスは登録時のまま返ってくる。
//
// ルートを空文字で表すという一点だけが特殊なので、
// 正規化と復元をここに閉じ込めておきます。
//
// "\\" は区切りとして扱いません。Dropbox はファイル名に含められる
// ふつうの文字なので、区切りに読み替えると "a\b.txt" という名前の
// ファイルが "a" の下の "b.txt" になってしまいます。
// Windows の慣れで "dropbox:\写真" と書いた場合は、
// 見つからないという失敗になります。

// normalize は hbg のパスを Dropbox API に渡せる形にします。
// ルートは空文字になります。
func normalize(p string) string {
	if p == "" || p == "/" || p == "." {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = path.Clean(p)
	if p == "/" || p == "." {
		return ""
	}
	return p
}

// display は API に渡した形のパスを hbg のパスに戻します。
// ルートの空文字を "/" に戻すためのものです。
func display(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// isRoot はルートを指すパスかどうかを返します。
func isRoot(p string) bool {
	return normalize(p) == ""
}

// parentOf は親ディレクトリを返します。ルートの親はルートです。
func parentOf(p string) string {
	p = normalize(p)
	if p == "" {
		return ""
	}
	return normalize(path.Dir(p))
}
