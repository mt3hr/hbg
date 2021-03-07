package hbg

import (
	"io"
	"time"
)

// Storage .
// ファイルストレージの抽象オブジェクトです
type Storage interface {
	// ディレクトリ内のファイルを列挙します。
	List(path string) ([]*FileInfo, error)

	// ファイルのメタデータを取得します。存在しなかった場合はエラーを返します。
	Stat(path string) (*FileInfo, error)

	// ファイルを取得します。存在しなかった場合はエラーを返します。かならずFile.Data.Close()してください。
	Get(path string) (*File, error)

	// 親ディレクトリを作成し、ファイルを作成します。
	// すでにファイルが存在する場合は上書きします。
	Push(dirPath string, data *File) error

	// pathとそれの中身をすべて削除します。
	Delete(path string) error

	// ディレクトリを作成します。
	MkDir(path string) error

	// ストレージタイプを取得します。例えばdropboxならdropboxです
	Type() string

	// ストレージ名を取得します。
	Name() string

	// このストレージを閉じます。
	Close() error
}

// FileInfo .
// ファイルのメタデータ
type FileInfo struct {
	Path  string // ファイルの存在するpath。filepath.Abs
	IsDir bool

	Name    string // ファイル名。filepath.Base
	LastMod time.Time
	Size    int64
}

// File .
// ファイル。
// Dataは使っても使わなくても必ず閉じてください。
type File struct {
	Data io.ReadCloser // 必ず閉じてください

	Name    string // ファイル名。filepath.Base
	LastMod time.Time
	Size    int64
}
