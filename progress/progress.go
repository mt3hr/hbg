// Package progress は転送の進みぐあいを利用者に見せる部分です。
//
// 転送エンジンはここで定義するインターフェースにだけ依存し、
// 実際の表示方法（端末に描くのか、行ログを流すのか）を知りません。
// ストレージの実装にも進捗の概念は持ち込みません。
// 計測は、エンジンが読み取りの流れに割り込む形で行います。
package progress

import (
	"io"
	"time"
)

// Summary は転送全体の集計です。
type Summary struct {
	// Transferred は転送したファイル数です。
	Transferred int
	// Skipped は転送不要と判断したファイル数です。
	Skipped int
	// Failed は転送に失敗したファイル数です。
	Failed int
	// Bytes は転送したバイト数です。
	Bytes int64
	// BytesSkipped は転送しなかったぶんのバイト数です。
	BytesSkipped int64
	// Elapsed は所要時間です。
	Elapsed time.Duration
	// Passes は実行した回数です（--retry-pass による再実行を含む）。
	Passes int
}

// Reporter は進みぐあいの表示先です。
//
// 各メソッドは複数のゴルーチンから同時に呼ばれます。
// 実装は自前で排他してください。
type Reporter interface {
	// ScanStarted は走査を始めたことを伝えます。
	ScanStarted()
	// ScanProgress は走査の途中経過を伝えます。
	// 走査と転送は並行するので、総量は途中で増えていきます。
	ScanProgress(dirs, files int64, bytes int64)
	// ScanDone は走査が終わったことを伝えます。
	ScanDone(dirs, files int64, bytes int64)

	// StartFile は1ファイルの転送を始めることを伝え、
	// そのファイル用の記録係を返します。
	// size は分からない場合 -1 です。
	StartFile(name string, size int64) FileTracker

	// Skipped は転送不要と判断したことを伝えます。
	Skipped(name string, size int64)

	// Logf は利用者へのメッセージを表示します。
	// 進捗の表示を壊さないように書き出します。
	Logf(format string, a ...any)

	// Done は転送が終わったことを伝えます。
	Done(s Summary)

	// Close は表示を後始末します。
	Close() error
}

// FileTracker は1ファイルぶんの記録係です。
type FileTracker interface {
	// Wrap は読み取りに割り込み、読んだ量を数えます。
	Wrap(r io.Reader) io.Reader
	// Reset は数えた量を0に戻します。再試行のときに使います。
	Reset()
	// Complete は、読み取りを経ずに転送が完了したことを伝えます。
	// サーバー側コピーのように内容が流れない場合に使います。
	Complete(n int64)
	// Abort は転送が失敗したことを伝えます。
	Abort()
	// Finish は1ファイルの処理が終わったことを伝えます。
	Finish()
}

// Nop は何も表示しない Reporter です。テストや --quiet で使います。
type Nop struct{}

// NewNop は何も表示しない Reporter を返します。
func NewNop() Reporter { return Nop{} }

func (Nop) ScanStarted()                        {}
func (Nop) ScanProgress(_, _ int64, _ int64)    {}
func (Nop) ScanDone(_, _ int64, _ int64)        {}
func (Nop) StartFile(string, int64) FileTracker { return nopTracker{} }
func (Nop) Skipped(string, int64)               {}
func (Nop) Logf(string, ...any)                 {}
func (Nop) Done(Summary)                        {}
func (Nop) Close() error                        { return nil }

type nopTracker struct{}

func (nopTracker) Wrap(r io.Reader) io.Reader { return r }
func (nopTracker) Reset()                     {}
func (nopTracker) Complete(int64)             {}
func (nopTracker) Abort()                     {}
func (nopTracker) Finish()                    {}
