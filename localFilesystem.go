package hbg

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

type LocalFilesystem struct{}

func (l *LocalFilesystem) List(path string) ([]*Path, error) {
	// 存在確認と、ディレクトリであるかの確認。
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrap(ErrPath, err.Error())
	}
	if !info.IsDir() {
		return nil, errors.Wrapf(ErrIsNotDir, "path = %s", path)
	}

	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	paths := []*Path{}
	for _, info := range infos {
		paths = append(paths, fileinfoToPath(path, info))
	}
	return paths, nil
}

func (l *LocalFilesystem) Stat(path string) (*Path, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrap(ErrPath, path)
	}
	return fileinfoToPath(path, info), nil
}

func fileinfoToPath(path string, info os.FileInfo) *Path {
	return &Path{
		Path:    filepath.ToSlash(filepath.Join(path, info.Name())),
		Name:    info.Name(),
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
}
func (l *LocalFilesystem) Get(path string) (*File, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrap(ErrPath, err.Error())
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &File{
		Name:    info.Name(),
		Data:    file,
		ModTime: info.ModTime(),
		Size:    info.Size(),
	}, nil
}

// 親ディレクトリは自動的に作成されます。
// modTimeの更新時にエラーが発生したときは、標準のロガーにPrintfして処理を続行します。
func (l *LocalFilesystem) Push(path string, data *File, override bool) error {
	// overrideの判定
	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			return errors.Wrapf(ErrPath, "%s is already exist. and is dir", path)
		}
		if !override {
			return errors.Wrap(ErrAlreadyExists, path)
		}
	}

	// 親ディレクトリを作成する
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}

	// 書き込み先ファイルを開く。
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, data.Data)
	if err != nil {
		return err
	}

	// 更新時刻を変更する
	err = os.Chtimes(path, time.Now(), data.ModTime)
	if err != nil {
		log.Printf("error: at os.Chtimes. err = %+v\n", err)
	}
	return nil
}

func (l *LocalFilesystem) Delete(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return errors.Wrap(ErrPath, err.Error())
	}
	if info.IsDir() {
		return os.RemoveAll(path)
	}
	return os.Remove(path)
}

func (l *LocalFilesystem) MkDir(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func (l *LocalFilesystem) Mv(from, to string) error {
	if _, err := os.Stat(to); err == nil {
		return errors.Wrap(ErrAlreadyExists, to)
	}
	if _, err := os.Stat(from); err != nil {
		return errors.Wrap(ErrPath, from)
	}
	return os.Rename(from, to)
}
