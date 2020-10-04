package hbg

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/jlaffaye/ftp"
)

type Storage interface {
	List(path string) ([]*FileInfo, error)

	// 存在しなかった場合はエラーを返します。
	Stat(path string) (*FileInfo, error)

	// 存在しなかった場合はエラーを返します。
	Get(path string) (*File, error)

	// 親ディレクトリを作成し、ファイルを作成します。
	// すでにファイルが存在する場合は上書きします。
	Push(dirPath string, data *File) error
	Delete(path string) error
	MkDir(path string) error
	Type() string
}

type FileInfo struct {
	Path  string // ファイルの存在するpath。filepath.Abs
	IsDir bool

	Name    string // ファイル名。filepath.Base
	LastMod time.Time
	Size    int64
}

type File struct {
	Data io.ReadCloser // 必ず閉じてください

	Name    string // ファイル名。filepath.Base
	LastMod time.Time
	Size    int64
}

type LocalFileSystem struct{}

func (l *LocalFileSystem) List(path string) ([]*FileInfo, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		err = fmt.Errorf("failed to read directory %s: %w", path, err)
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

func (l *LocalFileSystem) Stat(path string) (*FileInfo, error) {
	file, err := os.Stat(path)
	if err != nil {
		err = fmt.Errorf("failed to get stat %s: %w", path, err)
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

func (l *LocalFileSystem) Get(path string) (*File, error) {
	info, err := os.Stat(path)
	if err != nil {
		err = fmt.Errorf("failed to get stat %s: %w", path, err)
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

func (l *LocalFileSystem) Push(dirPath string, data *File) error {
	err := l.MkDir(dirPath)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", dirPath, err)
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
		err = fmt.Errorf("failed to write data to file %s: %w", file.Name(), err)
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

func (l *LocalFileSystem) Delete(path string) error {
	return os.RemoveAll(path)
}

func (l *LocalFileSystem) MkDir(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func (l *LocalFileSystem) Type() string {
	return "local"
}

type FTP struct {
	Conn *ftp.ServerConn
}

func (f *FTP) List(p string) ([]*FileInfo, error) {
	entries, err := f.Conn.List(p)
	if err != nil {
		err = fmt.Errorf("failed to list dir %s: %w", p, err)
		return nil, err
	}
	fileInfos := []*FileInfo{}
	for _, e := range entries {
		fileinfo := &FileInfo{
			Path:  path.Join(p, e.Name), //TODO
			IsDir: e.Type == ftp.EntryTypeFolder,

			Name:    e.Name,
			LastMod: e.Time,
			Size:    int64(e.Size),
		}
		fileInfos = append(fileInfos, fileinfo)
	}
	return fileInfos, nil
}
func (f *FTP) Stat(p string) (*FileInfo, error) {
	filename := path.Base(p)
	parentDir := path.Dir(p)
	infos, err := f.List(parentDir)
	if err != nil {
		err = fmt.Errorf("failed to list dir %s: %w", parentDir, err)
		return nil, err
	}

	info := &FileInfo{}
	exist := false
	for _, i := range infos {
		if i.Name == filename {
			info = i
			exist = true
			break
		}
	}
	if !exist {
		err = fmt.Errorf("not found %s")
		return nil, err
	}
	return info, nil
}
func (f *FTP) Get(p string) (*File, error) {
	info, err := f.Stat(p)
	if err != nil {
		err = fmt.Errorf("failed to get stat %s: %w", p, err)
		return nil, err
	}

	res, err := f.Conn.Retr(p)
	if err != nil {
		err = fmt.Errorf("failed to RETR %s: %w", p, err)
		return nil, err
	}

	file := &File{
		Data:    res,
		Name:    info.Name,
		LastMod: info.LastMod,
		Size:    info.Size,
	}
	return file, nil
}

// Lastmodの情報は消滅します
func (f *FTP) Push(dirPath string, data *File) error {
	filepath := path.Join(dirPath, data.Name)
	err := f.Conn.Stor(filepath, data.Data)
	if err != nil {
		err = fmt.Errorf("failed to Stor %s: %w", filepath, err)
		return err
	}
	return nil
}
func (f *FTP) Delete(path string) error {
	err := f.Conn.RemoveDirRecur(path)
	if err != nil {
		err := f.Conn.Delete(path)
		if err != nil {
			err = fmt.Errorf("failed to delete %s: %w", path, err)
			return err
		}
	}
	return nil
}
func (f *FTP) MkDir(path string) error {
	err := f.Conn.MakeDir(path)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", path, err)
		return err
	}
	return nil
}
func (f *FTP) Type() string {
	return "ftp"
}
