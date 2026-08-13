// Package storage は、ファイルストレージの抽象を定義します。
//
// 旧来の hbg.Storage との主な違いは次の4点です。
//
//   - すべてのメソッドが context.Context を受け取ります。
//     以前はキャンセルもタイムアウトもできず、Ctrl-C は書き込みの
//     途中でプロセスを殺すしかありませんでした。
//
//   - List が結果をコールバックへ流します。
//     全件をメモリに溜めずに済むため、件数の多いディレクトリでも
//     メモリ使用量が一定に保たれます。
//
//   - サイズ不明を SizeUnknown で表します。
//     以前は 0 が「空ファイル」と「サイズ不明」の両方を意味しており、
//     これが転送内容の切り詰めにつながっていました。
//
//   - できることを Features とオプショナルインターフェースで申告します。
//     ハッシュ検証やサーバー側コピーを、対応しているストレージでだけ
//     使い、対応していなければ通常の転送に落とせます。
package storage

import (
	"context"
	"io"
	"time"
)

// SizeUnknown はサイズが事前に分からないことを表します。
//
// 0（空ファイル）と区別するために負の値を使います。
// Dropbox への転送で内容が無警告に切り詰められていた不具合は、
// この区別がなかったことが原因でした。
const SizeUnknown int64 = -1

// FileInfo はストレージ上のファイルやディレクトリのメタデータです。
type FileInfo struct {
	// Path はストレージのルートからの絶対パスです。区切りは常に "/" です。
	Path string
	// Name はパスの最後の要素です。
	Name  string
	IsDir bool
	// Size はバイト数です。分からない場合は SizeUnknown です。
	Size int64
	// ModTime は最終更新時刻です。ゼロ値なら不明です。
	ModTime time.Time
	// Hashes は、一覧やメタデータの取得のついでに得られたハッシュです。
	// 追加の入出力を伴う取得は Hasher を使ってください。
	Hashes map[HashType]string
	// ID はバックエンド固有の識別子です（Google Drive の fileId など）。
	// 呼び出し側は中身を解釈しません。
	ID string
}

// ObjectMeta は書き込むファイルのメタデータです。
type ObjectMeta struct {
	// Size はバイト数です。分からない場合は SizeUnknown です。
	// 実装はこの値ではなく、実際に読み終わるまでを書き込んでください。
	Size int64
	// ModTime はゼロ値でなければ、可能なら書き込み先に反映します。
	ModTime time.Time
	// Hashes は転送元で分かっているハッシュです。
	// サーバー側で検証できる場合に使います。
	Hashes map[HashType]string
	// MIMEType は内容の種別です。分からなければ空で構いません。
	MIMEType string
}

// Storage はファイルストレージです。
//
// ここに並ぶのは、どのストレージでも実装できる最小限の操作だけです。
// 追加の能力は optional.go のインターフェースで表します。
type Storage interface {
	// Type はストレージの種別を返します（"local", "dropbox" など）。
	Type() string
	// Name は設定ファイルで付けた名前を返します。
	// 同じストレージかどうかの判定に使います。
	Name() string
	// Features はこのストレージにできることを返します。
	Features() *Features

	// List は dir の直下にあるものを fn に1件ずつ渡します。
	//
	// fn が非 nil を返したら列挙を打ち切り、その値を返します。
	// 実装はページ分割を内部で完結させ、全件をメモリに溜めないでください。
	// dir が存在しない場合は ErrNotFound を含むエラーを返します。
	List(ctx context.Context, dir string, fn func(FileInfo) error) error

	// Stat は1件のメタデータを返します。
	Stat(ctx context.Context, path string) (*FileInfo, error)

	// Open は path の内容を読むための ReadCloser を返します。
	// 呼び出し側が必ず Close してください。
	Open(ctx context.Context, path string) (io.ReadCloser, *FileInfo, error)

	// Put は path に r の内容を書き込みます。
	//
	//   - r は Close しないでください。所有権は呼び出し側にあります。
	//   - meta.Size は SizeUnknown のことがあります。
	//     宣言されたサイズではなく、読み終わるまでを書き込んでください。
	//   - すでにある場合は上書きします。
	//   - 途中経過が見えないよう、できるだけ不可分に書き込んでください。
	//   - ctx が取り消されたら、中途半端な結果を残さないでください。
	//
	// 戻り値は書き込み後の実際のメタデータです。
	Put(ctx context.Context, path string, r io.Reader, meta ObjectMeta) (*FileInfo, error)

	// Mkdir は dir を（必要なら親ごと）作ります。すでにあれば何もしません。
	Mkdir(ctx context.Context, dir string) error

	// Remove は1つのファイル、または空のディレクトリを削除します。
	// 中身ごと消したい場合は Purger または PurgeAll を使ってください。
	Remove(ctx context.Context, path string) error

	// Close はストレージを閉じます。
	Close() error
}

// Features はストレージにできることを表します。
//
// 「事前に方針を決める」ために使います。実際に呼び出せるかどうかは
// オプショナルインターフェースへの型アサーションで判定します。
// 両者の整合は適合性テストで検証します。
type Features struct {
	// ModTimePrecision は最終更新時刻の分解能です。
	// 比較の許容幅を決めるのに使います。
	// 例: ローカル(NTFS) 100ns / Dropbox 1s / Google Drive 1ms
	ModTimePrecision time.Duration
	// CanSetModTime は書き込み時に更新時刻を指定できるかどうかです。
	CanSetModTime bool
	// CaseInsensitive はパスの大文字小文字を区別しないかどうかです。
	CaseInsensitive bool
	// Hashes は追加の入出力なしで得られるハッシュの種類です。
	Hashes HashSet

	// ImplicitDirs は、Put が親ディレクトリを自動で作るかどうかです。
	ImplicitDirs bool
	// EmptyDirs は空のディレクトリを表現できるかどうかです。
	// オブジェクトストレージでは表現できないことがあります。
	EmptyDirs bool
	// AtomicPut は、書き込みの途中経過が観測されないかどうかです。
	AtomicPut bool
	// OSPath は、OS のパス規則（ドライブレターや UNC）に従うかどうかです。
	// 対話シェルでの補完やパス解決の判断に使います。
	OSPath bool

	// MaxFileSize は1ファイルの上限です。0 なら制限なしです。
	MaxFileSize int64
	// IllegalChars はファイル名に使えない文字です。
	IllegalChars string
}
