package hbg

import (
	"bufio"
	"io"
	"time"

	"github.com/pkg/errors"
)

// Storageの実装から投げられるerrorは、ハンドリングを容易とするために、
// ここに存在するものかまたはこれをwrapしたものとなるようにしてください。
// github.com/pkg/errors.Wrapです。
// ここにない場合は発生したそれを投げてもいいよ。
var (
	ErrIsNotDir      = errors.New("is not directory.")
	ErrIsDir         = errors.New("is directory.")
	ErrAlreadyExists = errors.New("file already exists.")
	ErrPath          = errors.New("path error.")
	ErrStorageStatus = errors.New("illegal storage status")
)

// pathの区切り文字は通常"/"です
// pathは基本的にすべて、操作対象を含む絶対パスです
type Storage interface {

	// pathの位置に存在するディレクトリ内のアイテムを取得します。
	// ディレクトリではない場合はエラーが飛びます。ErrIsNotDir
	List(path string) ([]*Path, error)

	Stat(path string) (*Path, error)

	// pathの位置に存在するファイルを取得します。
	// ディレクトリである場合はエラーが飛びます。ErrIsDir
	Get(path string) (*File, error)

	// pathの位置にファイルを書き込みます。
	// ディレクトリの場合はエラーが飛びます。
	// 書き込み先は第一引数のpathが使われ、第二引数のdata.Nameは無視されます。
	// 親ディレクトリが存在しなければ自動的に作成します。
	// 特に断りがなければModTimeもコピーします。
	// pushの成功失敗にかかわらず、data.DataはCloseされます。
	Push(path string, data *File) error

	// pathの示す位置に存在するファイルを削除します。
	// 対象がディレクトリの場合、それ以下のファイルも削除されます。
	Delete(path string) error

	// ディレクトリを作成します。
	// 親ディレクトリが存在しない場合、それも含めて作成します。
	MkDir(path string) error

	// Storage内のファイルを別の場所に移動します。
	// フォルダは内容も移動されます。
	// 移動先にすでにファイルが存在する場合はAlredyExistが飛びます。
	Mv(from, to string) error
}
type Path struct {
	Path  string
	Name  string
	IsDir bool

	Size    int64     //ディレクトリの場合は情報を持たない場合があります。
	ModTime time.Time //ディレクトリの場合は情報を持たない場合があります。
}
type File struct {
	Name    string
	Data    io.ReadCloser
	ModTime time.Time
	Size    int64
}

// このサイズより大きいデータのアレはbufio.Reader,Writerを使います。
var bufSize = bufio.NewReader(nil).Size() * 3

/*
 DeferErrorHandlerです。
 Deferで叩く関数がerrorを投げ得るときはこれに投げてください。
 それがerrorを返したとき、
 errがnilであればerrに代入します。
 errがnilでなければ元のerrorにmargeして返します。

 例えば、以下のようにして使います。
  func WriteHogeToFile(filename string) (err error) {
    file, _ := os.Open(filename)
    err = file.Write([]byte("hoge")
    if err != nil {
        return err
    }
    defer deh(func() error{
		return file.Close()
	}, &err)
	// 以下errorが発生しえる処理
  }
*/
func deh(f func() error, err *error) {
	e := f()
	if e == nil {
		return
	}
	if *err == nil {
		*err = e
	} else {
		*err = errors.Wrapf(e, "and err = %+v", e)
	}
}
