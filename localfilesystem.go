package hbg

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// NewLocalFileSystem .
// ディレクトリを読み込みます
func NewLocalFileSystem(dirname string) Storage {
	return &localFileSystem{
		name: dirname,
	}
}

type localFileSystem struct {
	name string
}

// ディレクトリ内のファイルを列挙します。
func (l *localFileSystem) List(path string) ([]*FileInfo, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		err = fmt.Errorf("error at read directory %s %s: %w", path, l.Name(), err)
		return nil, err
	}

	infos := []*FileInfo{}
	for _, file := range files {
		infos = append(infos, &FileInfo{
			Path:    filepath.ToSlash(filepath.Join(path, file.Name())),
			IsDir:   file.IsDir(),
			Name:    filepath.Base(file.Name()),
			Size:    file.Size(),
			LastMod: file.ModTime(),
		})
	}
	return infos, nil
}

// ファイルのメタデータを取得します。存在しなかった場合はエラーを返します。
func (l *localFileSystem) Stat(path string) (*FileInfo, error) {
	file, err := os.Stat(path)
	if err != nil {
		err = fmt.Errorf("error at get stat %s %s: %w", path, l.Name(), err)
		return nil, err
	}

	return &FileInfo{
		Path:    filepath.ToSlash(path),
		IsDir:   file.IsDir(),
		Name:    filepath.Base(file.Name()),
		Size:    file.Size(),
		LastMod: file.ModTime(),
	}, nil
}

// ファイルを取得します。存在しなかった場合はエラーを返します。かならずFile.Data.Close()してください。
func (l *localFileSystem) Get(path string) (*File, error) {
	info, err := os.Stat(path)
	if err != nil {
		err = fmt.Errorf("error at get stat %s %s: %w", path, l.Name(), err)
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		err = fmt.Errorf("filed to open file %s: %w", path, err)
		return nil, err
	}
	return &File{
		Data:    file,
		Name:    filepath.Base(file.Name()),
		LastMod: info.ModTime(),
		Size:    info.Size(),
	}, nil
}

// 親ディレクトリを作成し、ファイルを作成します。
// すでにファイルが存在する場合は上書きします。
func (l *localFileSystem) Push(dirPath string, data *File) error {
	err := l.MkDir(dirPath)
	if err != nil {
		err = fmt.Errorf("error at create directory %s %s: %w", dirPath, l.Name(), err)
		return err
	}

	path := filepath.Join(dirPath, data.Name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		err = fmt.Errorf("filed to open file %s: %w", path, err)
		return err
	}
	defer file.Close()
	defer data.Data.Close()

	_, err = io.Copy(file, data.Data)
	if err != nil {
		err = fmt.Errorf("error at write data to file %s %s: %w", file.Name(), l.Name(), err)
		return err
	}

	file.Close() // Closeしてからでないとchtimeが適用されないケースがあったため
	err = os.Chtimes(path, data.LastMod, data.LastMod)
	if err != nil {
		err = fmt.Errorf("filed to change file lastmod %s: %w", file.Name(), err)
		log.Printf("%s\n", err)
	}

	return nil
}

// pathとそれの中身をすべて削除します。
func (l *localFileSystem) Delete(path string) error {
	return os.RemoveAll(path)
}

// ディレクトリを作成します。
func (l *localFileSystem) MkDir(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

// ストレージタイプを取得します。例えばdropboxならdropboxです
func (l *localFileSystem) Type() string {
	return "local"
}

// ストレージ名を取得します。
func (l *localFileSystem) Name() string {
	return l.name
}

// このストレージを閉じます。
func (l *localFileSystem) Close() error {
	return nil
}
