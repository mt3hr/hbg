package hbg

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/pkg/errors"
)

type Storage interface {
	List(path string) (map[*FileInfo]interface{}, error)
	Stat(path string) (*FileInfo, error)

	Get(path string) (*File, error)
	Push(dirPath string, data *File) error
	Delete(path string) error
	MkDir(path string) error
	// Move(srcPath, destPath string) error
	Type() string
}

type FileInfo struct {
	Path  string
	IsDir bool

	Name    string
	LastMod time.Time
	Size    int64
}

type File struct {
	Data io.ReadCloser

	Name    string
	LastMod time.Time
	Size    int64
}

type LocalFileSystem struct{}

func (l *LocalFileSystem) List(path string) (map[*FileInfo]interface{}, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	infos := map[*FileInfo]interface{}{}
	for _, file := range files {
		infos[&FileInfo{
			Path:    filepath.ToSlash(filepath.Join(path, file.Name())),
			IsDir:   file.IsDir(),
			Name:    filepath.Base(file.Name()),
			Size:    file.Size(),
			LastMod: file.ModTime(),
		}] = struct{}{}
	}
	return infos, nil
}

func (l *LocalFileSystem) Stat(path string) (*FileInfo, error) {
	file, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &FileInfo{
		Path:    filepath.ToSlash(filepath.Join(path, file.Name())),
		IsDir:   file.IsDir(),
		Name:    filepath.Base(file.Name()),
		Size:    file.Size(),
		LastMod: file.ModTime(),
	}, nil
}

func (l *LocalFileSystem) Get(path string) (*File, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
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
		return err
	}

	path := filepath.Join(dirPath, data.Name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	defer data.Data.Close()

	_, err = io.Copy(file, data.Data)
	if err != nil {
		return err
	}

	file.Close() // Closeしてからでないとchtimeが適用されないケースがあったため
	err = os.Chtimes(path, data.LastMod, data.LastMod)
	if err != nil {
		return err
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

const (
	d_ChunkSize = 150 * 1048576    // 分割アップロードするかどうかの境界
	d_MaxSize   = 350 * 1073741824 // アップロード可能最大サイズ
)

// 時刻をDropboxの形式に丸めます。
// utcの秒までの情報です。
func TimeToDropbox(t time.Time) time.Time {
	return t.In(time.UTC).Truncate(time.Second)
}

// dropboxのラッパーです。
// ModTimeは秒まで丸められ、LocaleはUTCとなります。
// 比較するときにはTimeToDropbox関数を使ってください。
type Dropbox struct {
	Client dbx.Client
}

func (d *Dropbox) List(path string) (map[*FileInfo]interface{}, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}

	// pathがディレクトリであるかどうかの判断
	if path != "" {
		isDir, err := d.isDir(path)
		if err != nil {
			return nil, err
		}
		if !isDir {
			return nil, fmt.Errorf("%s is not Directory", path)
		}
	}

	metadatas, err := d.listFolder(path)
	if err != nil {
		return nil, err
	}
	fileInfos := map[*FileInfo]interface{}{}
	for _, metadata := range metadatas {
		fileInfo, err := metadataToFileInfo(metadata)
		if err != nil {
			return nil, err
		}
		fileInfos[fileInfo] = struct{}{}
	}
	return fileInfos, nil
}

func (d *Dropbox) isDir(path string) (bool, error) {
	// ディレクトリであるかどうかの判断
	arg := dbx.NewGetMetadataArg(path)
	metadata, err := d.Client.GetMetadata(arg)
	if err != nil {
		return false, err
	}
	fileInfo, err := metadataToFileInfo(metadata)
	if err != nil {
		return false, err
	}
	return fileInfo.IsDir, nil
}
func (d *Dropbox) listFolder(dirpath string) ([]dbx.IsMetadata, error) {
	client := d.Client
	res, err := client.ListFolder(dbx.NewListFolderArg(dirpath))
	if err != nil {
		return nil, err
	}
	metadatas := res.Entries
	for res.HasMore {
		res, err = client.ListFolderContinue(dbx.NewListFolderContinueArg(res.Cursor))
		if err != nil {
			return nil, err
		}
		metadatas = append(metadatas, res.Entries...)
	}
	return metadatas, nil
}
func metadataToFileInfo(metadata dbx.IsMetadata) (*FileInfo, error) {
	switch metadata.(type) {
	case *dbx.FolderMetadata:
		fo := metadata.(*dbx.FolderMetadata)
		return &FileInfo{
			IsDir: true,
			Name:  fo.Name,
			Path:  fo.PathLower,
		}, nil
	case *dbx.FileMetadata:
		fi := metadata.(*dbx.FileMetadata)
		return &FileInfo{
			IsDir:   false,
			Name:    fi.Name,
			Path:    fi.PathLower,
			Size:    int64(fi.Size),
			LastMod: fi.ClientModified,
		}, nil
	}
	err := errors.New("metadataがフォルダでもファイルでもありません")
	err = errors.Wrapf(err, "%+v", metadata)
	return nil, err
}

func (d *Dropbox) Stat(path string) (*FileInfo, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}

	metadata, err := d.Client.GetMetadata(dbx.NewGetMetadataArg(path))
	if err != nil {
		return nil, err
	}
	return metadataToFileInfo(metadata)
}
func (d *Dropbox) Get(path string) (*File, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}

	isDir, err := d.isDir(path)
	if err != nil {
		return nil, err
	}
	if isDir {
		return nil, fmt.Errorf("%s is Directory", path)
	}

	metadata, data, err := d.Client.Download(dbx.NewDownloadArg(path))
	if err != nil {
		return nil, err
	}
	return &File{
		Data:    data,
		Name:    metadata.Name,
		LastMod: metadata.ClientModified,
		Size:    int64(metadata.Size),
	}, nil
}
func (d *Dropbox) Push(dirPath string, data *File) error {
	path := filepath.ToSlash(filepath.Join(dirPath, data.Name))
	if err := d.pre(&path); err != nil {
		return err
	}

	// 大きすぎたらエラー
	if data.Size > d_MaxSize {
		return fmt.Errorf("%dbyte データのサイズが大きすぎます。%dbyte以内におさめてください。", data.Size, d_MaxSize)
	}

	// commitInfoを作る。timeはutcniにして秒で丸める
	commitInfo := dbx.NewCommitInfo(path)
	commitInfo.ClientModified = TimeToDropbox(data.LastMod)
	commitInfo.Autorename = false
	commitInfo.Mode = &dbx.WriteMode{Tagged: dropbox.Tagged{dbx.WriteModeOverwrite}}

	// 呼び出されるたびに次のチャンクをReaderとして返すやつ
	getNextChunk := func() io.Reader {
		chunk := io.LimitReader(data.Data, d_ChunkSize)
		return chunk
	}
	defer data.Data.Close()

	// ChunkSizeが150MNB以上と以下とで処理を分ける。
	// 150MB以上だと分割する必要があるので
	client := d.Client
	var err error
	if data.Size < d_ChunkSize {
		_, err = client.Upload(commitInfo, getNextChunk())
		if err != nil {
			return err
		}
		return nil
	} else {
		// 最初のチャンク
		sarg := dbx.NewUploadSessionStartArg()
		res, err := client.UploadSessionStart(sarg, getNextChunk())
		if err != nil {
			return err
		}

		// 最初、最後以外のチャンク
		uploaded := uint64(d_ChunkSize)
		datasize := uint64(data.Size)
		for datasize-uploaded > d_ChunkSize {
			c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
			aarg := dbx.NewUploadSessionAppendArg(c)

			err := client.UploadSessionAppendV2(aarg, getNextChunk())
			if err != nil {
				return err
			}
			uploaded += d_ChunkSize
		}

		// 最後のチャンク
		c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
		farg := dbx.NewUploadSessionFinishArg(c, commitInfo)
		_, err = client.UploadSessionFinish(farg, getNextChunk())
		return err
	}
}

func (d *Dropbox) Delete(path string) error {
	if err := d.pre(&path); err != nil {
		return err
	}

	_, err := d.Client.DeleteV2(dbx.NewDeleteArg(path))
	return err
}

func (d *Dropbox) MkDir(path string) error {
	if err := d.pre(&path); err != nil {
		return err
	}

	_, err := d.Client.CreateFolderV2(dbx.NewCreateFolderArg(path))
	return err

}

func (d *Dropbox) Move(srcPath, destPath string) error {
	arg := dbx.NewRelocationArg(srcPath, destPath)
	_, err := d.Client.Move(arg)
	return err
}

func (d *Dropbox) Type() string {
	return "dropbox"
}

func (d *Dropbox) pre(path *string) error {
	*path = filepath.ToSlash(*path)
	// ルートディレクトリは空文字で表現する
	if *path == "/" {
		*path = ""
	}
	return nil
}
