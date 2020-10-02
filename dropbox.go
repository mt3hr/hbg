package hbg

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dbxapi "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/google/uuid"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/oauth2"
)

var (
	d_ChunkSize uint64 = 150 * 1048576    // 分割アップロードするかどうかの境界
	d_MaxSize   uint64 = 350 * 1073741824 // アップロード可能最大サイズ
)

// 時刻をDropboxの形式に丸めます。
// utcの秒までの情報です。
func TimeToDropbox(t time.Time) time.Time {
	return t.In(time.UTC).Truncate(time.Second)
}

func NewDropbox(name string) (Storage, error) {
	token, err := loadTokenFromName(name)
	if err != nil {
		err = fmt.Errorf("load token filed. %w", err)
		return nil, err
	}

	client := dbx.New(dbxapi.Config{Token: token})
	return &dropbox{Client: client}, nil
}

func loadTokenFromName(name string) (string, error) {
	tokenFileName := fmt.Sprintf("hbg_token_%s_%s.json", "dropbox", name)
	home, err := homedir.Dir()
	if err != nil {
		err = fmt.Errorf("failed to get user home directory: %w", err)
		return "", err
	}
	exe, err := os.Executable()
	if err != nil {
		err = fmt.Errorf("failed to get execute directory: %w", err)
		return "", err
	}
	exe = filepath.Dir(exe)
	current := "."

	tok := &oauth2.Token{}
	for _, tokenDir := range []string{home, exe, current} {
		tokenFilePath := filepath.Join(tokenDir, tokenFileName)
		tok, err = tokenFromFile(tokenFilePath)
		if err == nil {
			break
		}
	}
	if err != nil {
		authCfg := &oauth2.Config{
			ClientID:     "9h7aole3khc6fb1",
			ClientSecret: "2njvqglp3o74q0s",
			Scopes:       []string{},
			Endpoint: oauth2.Endpoint{
				AuthURL:  "https://www.dropbox.com/oauth2/authorize",
				TokenURL: "https://api.dropboxapi.com/oauth2/token",
			},
		}
		authorizeUrl := authCfg.AuthCodeURL(
			uuid.New().String(),
			oauth2.SetAuthURLParam("response_type", "code"),
		)

		fmt.Printf("%s: %s の初期化を行います。\n下記のURLを開いてhbgを許可し、表示されたキーをこの画面に貼り付けてください。\n%s\n", "dropbox", name, authorizeUrl)
		var code string
		fmt.Scan(&code)
		tok, err = authCfg.Exchange(context.Background(), code)
		if err != nil {
			err = fmt.Errorf("failed exchange auth cfg. %w", err)
			return "", err
		}
		tokenFilePath := filepath.Join(home, tokenFileName)
		err = saveToken(tokenFilePath, tok)
		if err != nil {
			err = fmt.Errorf("failed save token. %w", err)
			return "", err
		}
	}
	return tok.AccessToken, nil
}

// dropboxのラッパーです。
// ModTimeは秒まで丸められ、LocaleはUTCとなります。
// 比較するときにはTimeToDropbox関数を使ってください。
type dropbox struct {
	Client dbx.Client
}

func (d *dropbox) List(path string) (map[*FileInfo]interface{}, error) {
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

func (d *dropbox) isDir(path string) (bool, error) {
	// ディレクトリであるかどうかの判断
	arg := dbx.NewGetMetadataArg(path)
	metadata, err := d.Client.GetMetadata(arg)
	if err != nil {
		err = fmt.Errorf("failed to get %s metadata at dropbox: %w", path, err)
		return false, err
	}
	fileInfo, err := metadataToFileInfo(metadata)
	if err != nil {
		err = fmt.Errorf("failed to metadataToFileInfo %s: %w", path, err)
		return false, err
	}
	return fileInfo.IsDir, nil
}

func (d *dropbox) listFolder(dirpath string) (map[dbx.IsMetadata]interface{}, error) {
	client := d.Client
	res, err := client.ListFolder(dbx.NewListFolderArg(dirpath))
	if err != nil {
		err = fmt.Errorf("failed to list folder %s at dropbox: %w", dirpath, err)
		return nil, err
	}
	metadatas := map[dbx.IsMetadata]interface{}{}
	for _, metadata := range res.Entries {
		metadatas[metadata] = struct{}{}
	}

	for res.HasMore {
		res, err = client.ListFolderContinue(dbx.NewListFolderContinueArg(res.Cursor))
		if err != nil {
			err = fmt.Errorf("failed to list folder more %s at dropbox: %w", dirpath, err)
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
	err := fmt.Errorf("metadata is not folder and file. metadata=%s", metadata)
	return nil, err
}

func (d *dropbox) Stat(path string) (*FileInfo, error) {
	if err := d.pre(&path); err != nil {
		return nil, err
	}

	metadata, err := d.Client.GetMetadata(dbx.NewGetMetadataArg(path))
	if err != nil {
		err = fmt.Errorf("failed to get %s metadata from dropbox: %w", path, err)
		return nil, err
	}
	return metadataToFileInfo(metadata)
}

func (d *dropbox) Get(path string) (*File, error) {
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
		err = fmt.Errorf("failed to download %s from dropbox: %w", path, err)
		return nil, err
	}
	return &File{
		Data:    data,
		Name:    metadata.Name,
		LastMod: metadata.ClientModified,
		Size:    int64(metadata.Size),
	}, nil
}

func (d *dropbox) Push(dirPath string, data *File) error {
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
	commitInfo.Mode = &dbx.WriteMode{Tagged: dbxapi.Tagged{dbx.WriteModeOverwrite}}

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
			err = fmt.Errorf("failed to upload %s at dropbox: %w", path, err)
			return err
		}
		return nil
	} else {
		// 最初のチャンク
		sarg := dbx.NewUploadSessionStartArg()
		res, err := client.UploadSessionStart(sarg, getNextChunk())
		if err != nil {
			err = fmt.Errorf("failed to upload session start %s at dropbox: %w", path, err)
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
				err = fmt.Errorf("failed to upload session append %s at dropbox: %w", path, err)
				return err
			}
			uploaded += d_ChunkSize
		}

		// 最後のチャンク
		c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
		farg := dbx.NewUploadSessionFinishArg(c, commitInfo)
		_, err = client.UploadSessionFinish(farg, getNextChunk())
		if err != nil {
			err = fmt.Errorf("failed to upload session finish %s at dropbox: %w", path, err)
			return err
		}
		return nil
	}
}

func (d *dropbox) Delete(path string) error {
	if err := d.pre(&path); err != nil {
		return err
	}

	_, err := d.Client.DeleteV2(dbx.NewDeleteArg(path))
	if err != nil {
		err = fmt.Errorf("failed to delete %s: %w", path, err)
		return err
	}
	return nil
}

func (d *dropbox) MkDir(path string) error {
	time.Sleep(time.Second * 2) // mkdirしすぎると怒られるので
	if err := d.pre(&path); err != nil {
		return err
	}

	_, err := d.Client.CreateFolderV2(dbx.NewCreateFolderArg(path))
	if err != nil {
		err = fmt.Errorf("failed to create folder %s at dropbox: %w", path, err)
		return err
	}
	return nil
}

func (d *dropbox) Move(srcPath, destPath string) error {
	arg := dbx.NewRelocationArg(srcPath, destPath)
	_, err := d.Client.Move(arg)
	if err != nil {
		err = fmt.Errorf("failed to move files from %s to %s at dropbox: %w", srcPath, destPath, err)
		return err
	}
	return nil
}

func (d *dropbox) Type() string {
	return "dropbox"
}

func (d *dropbox) pre(path *string) error {
	*path = filepath.ToSlash(*path)
	// ルートディレクトリは空文字で表現する
	if *path == "/" {
		*path = ""
	}
	return nil
}
