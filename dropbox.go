package hbg

import (
	"bufio"
	"io"
	"path/filepath"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/pkg/errors"
)

// dropboxのラッパーです。
// ModTimeは秒まで丸められ、LocaleはUTCとなります。
// 比較するときにはTimeToDropbox関数を使ってください。
type Dropbox struct {
	Client dbx.Client
}

// 時刻をDropboxの形式に丸めます。
// utcの秒までの情報です。
func TimeToDropbox(t time.Time) time.Time {
	return t.In(time.UTC).Truncate(time.Second)
}

const (
	d_ChunkSize = 150 * 1048576    // 分割アップロードするかどうかの境界
	d_MaxSize   = 350 * 1073741824 // アップロード可能最大サイズ
)

func (d *Dropbox) Push(path string, data *File) (err error) {
	if err = d.pre(&path); err != nil {
		return err
	}

	// 大きすぎたら返す
	if data.Size > d_MaxSize {
		err = errors.New("can not push to dropbox.")
		err = errors.Wrapf(err, "data is to large. max size is %d byte", d_MaxSize)
		err = errors.Wrapf(err, "size=%d", data.Size)
		return err
	}

	// commitinfo。 timeはutcにしてから秒で丸める
	commitinfo := dbx.NewCommitInfo(path)
	commitinfo.ClientModified = TimeToDropbox(data.ModTime)
	commitinfo.Autorename = false
	commitinfo.Mode = &dbx.WriteMode{Tagged: dropbox.Tagged{dbx.WriteModeOverwrite}}

	// 呼び出されるたびに次のチャンクをReaderとして返します
	getNextChunk := func() io.Reader {
		chunk := io.LimitReader(data.Data, d_ChunkSize)
		if data.Size > int64(bufSize) {
			chunk = bufio.NewReader(chunk)
		}
		return chunk
	}
	defer deh(func() error { return data.Data.Close() }, &err)

	// ChunkSize(150MB)以上と以下で分ける
	client := d.Client
	if data.Size < d_ChunkSize {
		_, err = client.Upload(commitinfo, getNextChunk())
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
		farg := dbx.NewUploadSessionFinishArg(c, commitinfo)
		_, err = client.UploadSessionFinish(farg, getNextChunk())
		return err
	}
}
func (d *Dropbox) List(path string) ([]*Path, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}

	if path != "" {
		isdir, err := d.isDir(path)
		if err != nil {
			return nil, err
		}
		if !isdir {
			return nil, errors.Wrapf(ErrIsNotDir, path)
		}
	}

	// pathsを作って返す
	metadatas, err := d.listFolder(path)
	if err != nil {
		return nil, errors.Wrapf(err, path)
	}
	paths := []*Path{}
	for _, metadata := range metadatas {
		path, err := metadataToPath(metadata)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}
func (d *Dropbox) Stat(path string) (*Path, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}
	metadata, err := d.Client.GetMetadata(dbx.NewGetMetadataArg(path))
	if err != nil {
		return nil, errors.Wrap(ErrPath, err.Error())
	}
	return metadataToPath(metadata)
}
func (d *Dropbox) Get(path string) (*File, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}
	if isdir, err := d.isDir(path); err != nil {
		return nil, err
	} else if isdir {
		return nil, errors.Wrapf(ErrIsDir, path)
	}

	metadata, data, err := d.Client.Download(dbx.NewDownloadArg(path))
	if err != nil {
		return nil, err
	}

	return &File{
		Data:    data,
		ModTime: metadata.ClientModified,
		Name:    metadata.Name,
		Size:    int64(metadata.Size),
	}, nil
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
	_, err := d.Client.CreateFolder(dbx.NewCreateFolderArg(path))
	return err
}
func (d *Dropbox) Mv(from, to string) error {
	if _, err := d.Stat(to); err == nil {
		return errors.Wrap(ErrAlreadyExists, to)
	}
	if _, err := d.Stat(from); err != nil {
		return errors.Wrap(ErrPath, from)
	}
	arg := dbx.NewRelocationArg(from, to)
	_, err := d.Client.Move(arg)
	return err
}

func (d *Dropbox) isDir(path string) (bool, error) {
	if err := d.pre(&path); err != nil {
		return false, err
	}

	// ディレクトリであるかどうかの判断
	arg := dbx.NewGetMetadataArg(path)
	metadata, err := d.Client.GetMetadata(arg)
	if err != nil {
		return false, errors.Wrap(ErrPath, err.Error())
	}
	p, err := metadataToPath(metadata)
	if err != nil {
		return false, err
	}
	return p.IsDir, nil
}
func (d *Dropbox) listFolder(dirpath string) ([]dbx.IsMetadata, error) {
	if err := d.pre(&dirpath); err != nil {
		return nil, err
	}

	client := d.Client
	res, err := client.ListFolder(dbx.NewListFolderArg(dirpath))
	if err != nil {
		return nil, err
	}
	metadatas := res.Entries
	for res.HasMore {
		res, err = client.ListFolderContinue(dbx.NewListFolderContinueArg(res.Cursor))
		if err != nil {
			return nil, errors.Wrapf(ErrStorageStatus, "has more data but err. err=%+v", err)
		}
		metadatas = append(metadatas, res.Entries...)
	}
	return metadatas, nil
}

// pathをスラッシュにしたり、
// clientのnilチェックをしたり。
func (d *Dropbox) pre(path *string) error {
	if d.Client == nil {
		return errors.Wrap(ErrStorageStatus, "client is nil")
	}

	*path = filepath.ToSlash(*path)
	// ルートディレクトリは空文字で表現する
	if *path == "/" {
		*path = ""
	}
	return nil
}
func metadataToPath(metadata dbx.IsMetadata) (*Path, error) {
	switch metadata.(type) {
	case *dbx.FolderMetadata:
		fo := metadata.(*dbx.FolderMetadata)
		return &Path{
			IsDir: true,
			Name:  fo.Name,
			Path:  fo.PathLower,
		}, nil
	case *dbx.FileMetadata:
		fi := metadata.(*dbx.FileMetadata)
		return &Path{
			IsDir:   false,
			Name:    fi.Name,
			Path:    fi.PathLower,
			Size:    int64(fi.Size),
			ModTime: fi.ClientModified,
		}, nil
	}
	err := errors.New("metadataがフォルダでもファイルでもありません")
	err = errors.Wrapf(err, "%+v", metadata)
	return nil, err
}
