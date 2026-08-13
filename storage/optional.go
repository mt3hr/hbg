package storage

import (
	"context"
	"io"
	"time"
)

// ここに並ぶインターフェースは、ストレージによって実装できたりできなかったり
// する能力を表します。呼び出し側は helpers.go のヘルパを通して使い、
// 型アサーションを各所に散らさないようにします。

// Hasher は、追加の入出力を伴ってでもハッシュを取得できるストレージです。
type Hasher interface {
	Hash(ctx context.Context, path string, ht HashType) (string, error)
}

// ServerSideCopier は、内容を転送せずにコピーできるストレージです。
// 同じストレージの中でのみ使えます。
type ServerSideCopier interface {
	ServerSideCopy(ctx context.Context, srcPath, dstPath string) (*FileInfo, error)
}

// Mover は、内容を転送せずに移動・改名できるストレージです。
type Mover interface {
	Move(ctx context.Context, srcPath, dstPath string) error
}

// RangeOpener は、途中から読み出せるストレージです。
// 再開や再試行の最適化に使います。
type RangeOpener interface {
	// OpenRange は offset から length バイトを読む ReadCloser を返します。
	// length が負なら最後までを読みます。
	OpenRange(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
}

// Purger は、ディレクトリを中身ごと削除できるストレージです。
// 1回の呼び出しで済むストレージ向けです。
type Purger interface {
	Purge(ctx context.Context, dir string) error
}

// SetModTimer は、書き込み後に最終更新時刻だけを変更できるストレージです。
type SetModTimer interface {
	SetModTime(ctx context.Context, path string, t time.Time) error
}
