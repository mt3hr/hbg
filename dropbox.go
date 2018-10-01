package hbg

import (
	"io"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/pkg/errors"
)

type Dropbox struct {
	Client dbx.Client
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

// overrideしなかった場合、エラーを返しますが、それはErrAlreadyExistを返しません。
func (d *Dropbox) Push(path string, data *File, override bool) (err error) {
	if err = d.pre(&path); err != nil {
		return err
	}

	var (
		Border    int64  = 150 * 1048576
		ChunkSize uint64 = uint64(Border)
		OverSize  int64  = 350 * 1073741824
	)
	// 大きすぎたら返す
	if int64(data.Size) > OverSize {
		err = errors.New("data is to large. max size = " + strconv.FormatInt(OverSize, 10) + "byte.")
		err = errors.Wrapf(err, "size=%d", data.Size)
		return err
	}

	// commitinfo。 timeはutcにしてから秒で丸める
	commitinfo := dbx.NewCommitInfo(path)
	commitinfo.ClientModified = data.ModTime.In(time.UTC).Truncate(time.Second)
	commitinfo.Autorename = false
	// override判定
	if override {
		overridemode := &dbx.WriteMode{Tagged: dropbox.Tagged{dbx.WriteModeOverwrite}}
		commitinfo.Mode = overridemode
	}

	defer func() {
		e := data.Data.Close()
		if e != nil {
			err = errors.Wrap(err, e.Error())
		}
	}()

	// 150MB以上と以下で分ける
	client := d.Client
	if data.Size < Border {
		_, err = client.Upload(commitinfo, data.Data)
		if err != nil {
			return err
		}
		return nil
	} else {
		// 最初のチャンク
		limReader := io.LimitReader(data.Data, int64(ChunkSize))
		sarg := dbx.NewUploadSessionStartArg()
		res, err := client.UploadSessionStart(sarg, limReader)
		if err != nil {
			return err
		}

		// 最初、最後以外のチャンク
		uploaded := ChunkSize
		for uint64(data.Size)-uploaded > ChunkSize {
			c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
			limReader = io.LimitReader(data.Data, int64(ChunkSize))

			aarg := dbx.NewUploadSessionAppendArg(c)
			err := client.UploadSessionAppendV2(aarg, limReader)
			if err != nil {
				return err
			}
			uploaded += ChunkSize
		}

		// 最後のチャンク
		limReader = io.LimitReader(data.Data, int64(ChunkSize))
		c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
		farg := dbx.NewUploadSessionFinishArg(c, commitinfo)
		_, err = client.UploadSessionFinish(farg, limReader)
		return err
	}
	return nil
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
