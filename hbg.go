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

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/jlaffaye/ftp"
	errors "golang.org/x/xerrors"
)

type Storage interface {
	List(path string) (map[*FileInfo]interface{}, error)

	// 存在しなかった場合はエラーを返します。
	Stat(path string) (*FileInfo, error)

	// 存在しなかった場合はエラーを返します。
	Get(path string) (*File, error)

	// 親ディレクトリを作成し、ファイルを作成します。
	// すでにファイルが存在する場合は上書きします。
	Push(dirPath string, data *File) error
	Delete(path string) error
	MkDir(path string) error
	// Move(srcPath, destPath string) error
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

func (l *LocalFileSystem) List(path string) (map[*FileInfo]interface{}, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		err = errors.Errorf("failed to read directory %s: %w", path, err)
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
		err = errors.Errorf("failed to get stat %s: %w", path, err)
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
		err = errors.Errorf("failed to get stat %s: %w", path, err)
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		err = errors.Errorf("filed to open file %s: %w", path, err)
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
		err = errors.Errorf("failed to create directory %s: %w", dirPath, err)
		return err
	}

	path := filepath.Join(dirPath, data.Name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		err = errors.Errorf("filed to open file %s: %w", path, err)
		return err
	}
	defer file.Close()
	defer data.Data.Close()

	_, err = io.Copy(file, data.Data)
	if err != nil {
		err = errors.Errorf("failed to write data to file %s: %w", file.Name(), err)
		return err
	}

	file.Close() // Closeしてからでないとchtimeが適用されないケースがあったため
	err = os.Chtimes(path, data.LastMod, data.LastMod)
	if err != nil {
		err = errors.Errorf("filed to change file lastmod %s: %w", file.Name(), err)
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

var (
	d_ChunkSize uint64 = 150 * 1048576    // 分割アップロードするかどうかの境界
	d_MaxSize   uint64 = 350 * 1073741824 // アップロード可能最大サイズ
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
	for metadata := range metadatas {
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
		err = errors.Errorf("failed to get %s metadata at dropbox: %w", path, err)
		return false, err
	}
	fileInfo, err := metadataToFileInfo(metadata)
	if err != nil {
		err = errors.Errorf("failed to metadataToFileInfo %s: %w", path, err)
		return false, err
	}
	return fileInfo.IsDir, nil
}

func (d *Dropbox) listFolder(dirpath string) (map[dbx.IsMetadata]interface{}, error) {
	client := d.Client
	res, err := client.ListFolder(dbx.NewListFolderArg(dirpath))
	if err != nil {
		err = errors.Errorf("failed to list folder %s at dropbox: %w", dirpath, err)
		return nil, err
	}
	metadatas := map[dbx.IsMetadata]interface{}{}
	for _, metadata := range res.Entries {
		metadatas[metadata] = struct{}{}
	}

	for res.HasMore {
		res, err = client.ListFolderContinue(dbx.NewListFolderContinueArg(res.Cursor))
		if err != nil {
			err = errors.Errorf("failed to list folder more %s at dropbox: %w", dirpath, err)
			return nil, err
		}
		for _, metadata := range res.Entries {
			metadatas[metadata] = struct{}{}
		}
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
	err := errors.Errorf("metadata is not folder and file. metadata=%s", metadata)
	return nil, err
}

func (d *Dropbox) Stat(path string) (*FileInfo, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}

	metadata, err := d.Client.GetMetadata(dbx.NewGetMetadataArg(path))
	if err != nil {
		err = errors.Errorf("failed to get %s metadata from dropbox: %w", path, err)
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
		err := fmt.Errorf("%s is Directory", path)
		return nil, err
	}

	metadata, data, err := d.Client.Download(dbx.NewDownloadArg(path))
	if err != nil {
		err = errors.Errorf("failed to download %s from dropbox: %w", path, err)
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
	if uint64(data.Size) > d_MaxSize {
		return fmt.Errorf("%dbyte データのサイズが大きすぎます。%dbyte以内におさめてください。", data.Size, d_MaxSize)
	}

	// commitInfoを作る。timeはutcniにして秒で丸める
	commitInfo := dbx.NewCommitInfo(path)
	commitInfo.ClientModified = TimeToDropbox(data.LastMod)
	commitInfo.Autorename = false
	commitInfo.Mode = &dbx.WriteMode{Tagged: dropbox.Tagged{dbx.WriteModeOverwrite}}

	// 呼び出されるたびに次のチャンクをReaderとして返すやつ
	getNextChunk := func() io.Reader {
		chunk := io.LimitReader(data.Data, int64(d_ChunkSize))
		return chunk
	}
	defer data.Data.Close()

	// ChunkSizeが150MNB以上と以下とで処理を分ける。
	// 150MB以上だと分割する必要があるので
	client := d.Client
	var err error
	if uint64(data.Size) < d_ChunkSize {
		_, err = client.Upload(commitInfo, getNextChunk())
		if err != nil {
			err = errors.Errorf("failed to upload %s at dropbox: %w", path, err)
			return err
		}
		return nil
	} else {
		// 最初のチャンク
		sarg := dbx.NewUploadSessionStartArg()
		res, err := client.UploadSessionStart(sarg, getNextChunk())
		if err != nil {
			err = errors.Errorf("failed to upload session start %s at dropbox: %w", path, err)
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
				err = errors.Errorf("failed to upload session append %s at dropbox: %w", path, err)
				return err
			}
			uploaded += d_ChunkSize
		}

		// 最後のチャンク
		c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
		farg := dbx.NewUploadSessionFinishArg(c, commitInfo)
		_, err = client.UploadSessionFinish(farg, getNextChunk())
		if err != nil {
			err = errors.Errorf("failed to upload session finish %s at dropbox: %w", path, err)
			return err
		}
		return nil
	}
}

func (d *Dropbox) Delete(path string) error {
	if err := d.pre(&path); err != nil {
		return err
	}

	_, err := d.Client.DeleteV2(dbx.NewDeleteArg(path))
	if err != nil {
		err = errors.Errorf("failed to delete %s: %w", path, err)
		return err
	}
	return nil
}

func (d *Dropbox) MkDir(path string) error {
	if err := d.pre(&path); err != nil {
		return err
	}

	_, err := d.Client.CreateFolderV2(dbx.NewCreateFolderArg(path))
	if err != nil {
		err = errors.Errorf("failed to create folder %s at dropbox: %w", path, err)
		return err
	}
	return nil
}

func (d *Dropbox) Move(srcPath, destPath string) error {
	arg := dbx.NewRelocationArg(srcPath, destPath)
	_, err := d.Client.Move(arg)
	if err != nil {
		err = errors.Errorf("failed to move files from %s to %s at dropbox: %w", srcPath, destPath, err)
		return err
	}
	return nil
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

type FTP struct {
	Conn *ftp.ServerConn
}

func (f *FTP) List(p string) (map[*FileInfo]interface{}, error) {
	entries, err := f.Conn.List(p)
	if err != nil {
		err = errors.Errorf("failed to list dir %s: %w", p, err)
		return nil, err
	}
	fileInfos := map[*FileInfo]interface{}{}
	for _, e := range entries {
		fileinfo := &FileInfo{
			Path:  path.Join(p, e.Name), //TODO
			IsDir: e.Type == ftp.EntryTypeFolder,

			Name:    e.Name,
			LastMod: e.Time,
			Size:    int64(e.Size),
		}
		fileInfos[fileinfo] = struct{}{}
	}
	return fileInfos, nil
}
func (f *FTP) Stat(p string) (*FileInfo, error) {
	filename := path.Base(p)
	parentDir := path.Dir(p)
	infos, err := f.List(parentDir)
	if err != nil {
		err = errors.Errorf("failed to list dir %s: %w", parentDir, err)
		return nil, err
	}

	info := &FileInfo{}
	exist := false
	for i := range infos {
		if i.Name == filename {
			info = i
			exist = true
			break
		}
	}
	if !exist {
		err = errors.Errorf("not found %s")
		return nil, err
	}
	return info, nil
}
func (f *FTP) Get(p string) (*File, error) {
	info, err := f.Stat(p)
	if err != nil {
		err = errors.Errorf("failed to get stat %s: %w", p, err)
		return nil, err
	}

	res, err := f.Conn.Retr(p)
	if err != nil {
		err = errors.Errorf("failed to RETR %s: %w", p, err)
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
		err = errors.Errorf("failed to Stor %s: %w", filepath, err)
		return err
	}
	return nil
}
func (f *FTP) Delete(path string) error {
	err := f.Conn.RemoveDirRecur(path)
	if err != nil {
		err := f.Conn.Delete(path)
		if err != nil {
			err = errors.Errorf("failed to delete %s: %w", path, err)
			return err
		}
	}
	return nil
}
func (f *FTP) MkDir(path string) error {
	err := f.Conn.MakeDir(path)
	if err != nil {
		err = errors.Errorf("failed to create directory %s: %w", path, err)
		return err
	}
	return nil
}
func (f *FTP) Type() string {
	return "ftp"
}
