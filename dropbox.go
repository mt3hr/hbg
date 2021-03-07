package hbg

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	dbxapi "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/google/uuid"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/oauth2"
)

var (
	dropboxChunkSize uint64 = 150 * 1048576    // 分割アップロードするかどうかの境界
	dropboxMaxSize   uint64 = 350 * 1073741824 // アップロード可能最大サイズ
)

// TimeToDropbox .
// 時刻をDropboxの形式に丸めます。
// utcの秒までの情報です。
func TimeToDropbox(t time.Time) time.Time {
	return t.In(time.UTC).Truncate(time.Second)
}

// NewDropbox .
// dropboxを読み込みます。
// nameは任意の名前です。
// 初回起動時にコマンドライン入力を求められ、
// $HOME/hbg_config_$name.yamlにキーが保存され、以後楽に接続できるようになります。
func NewDropbox(name string) (Storage, error) {
	token, err := loadTokenFromName(name)
	if err != nil {
		err = fmt.Errorf("load token filed. %w", err)
		return nil, err
	}

	client := dbx.New(dbxapi.Config{Token: token})
	return &dropbox{
		Client: client,
		name:   name,
	}, nil
}

// dropboxのラッパーです。
// ModTimeは秒まで丸められ、LocaleはUTCとなります。
// 比較するときにはTimeToDropbox関数を使ってください。
type dropbox struct {
	Client dbx.Client
	name   string
}

// ディレクトリ内のファイルを列挙します。
func (d *dropbox) List(filepath string) ([]*FileInfo, error) {
	dir, f := path.Split(filepath)
	filepath = path.Join(dir, f)

	if err := d.pre(&filepath); err != nil {
		return nil, err
	}

	// pathがディレクトリであるかどうかの判断
	if filepath != "" {
		isDir, err := d.isDir(filepath)
		if err != nil {
			return nil, err
		}
		if !isDir {
			return nil, fmt.Errorf("%s is not Directory", filepath)
		}
	}

	metadatas, err := d.listFolder(filepath)
	if err != nil {
		return nil, err
	}
	fileInfos := []*FileInfo{}
	for metadata := range metadatas {
		fileInfo, err := metadataToFileInfo(metadata)
		if err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, fileInfo)
	}
	return fileInfos, nil
}

// ファイルのメタデータを取得します。存在しなかった場合はエラーを返します。
func (d *dropbox) Stat(filepath string) (*FileInfo, error) {
	dir, f := path.Split(filepath)
	filepath = path.Join(dir, f)

	if filepath == "/" {
		return &FileInfo{
			IsDir: true,
			Name:  "/",
			Path:  "/",
		}, nil
	}

	if err := d.pre(&filepath); err != nil {
		return nil, err
	}

	metadata, err := d.Client.GetMetadata(dbx.NewGetMetadataArg(filepath))
	if err != nil {
		err = fmt.Errorf("failed to get %s metadata from dropbox: %w", filepath, err)
		return nil, err
	}
	return metadataToFileInfo(metadata)
}

// ファイルを取得します。存在しなかった場合はエラーを返します。かならずFile.Data.Close()してください。
func (d *dropbox) Get(filepath string) (*File, error) {
	dir, f := path.Split(filepath)
	filepath = path.Join(dir, f)

	if err := d.pre(&filepath); err != nil {
		return nil, err
	}

	isDir, err := d.isDir(filepath)
	if err != nil {
		return nil, err
	}
	if isDir {
		err := fmt.Errorf("%s is Directory", filepath)
		return nil, err
	}

	metadata, data, err := d.Client.Download(dbx.NewDownloadArg(filepath))
	if err != nil {
		err = fmt.Errorf("failed to download %s from dropbox: %w", filepath, err)
		return nil, err
	}
	return &File{
		Data:    data,
		Name:    metadata.Name,
		LastMod: metadata.ClientModified,
		Size:    int64(metadata.Size),
	}, nil
}

// 親ディレクトリを作成し、ファイルを作成します。
// すでにファイルが存在する場合は上書きします。
func (d *dropbox) Push(dirPath string, data *File) error {
	dir, f := path.Split(dirPath)
	dirPath = path.Join(dir, f)

	path := filepath.ToSlash(filepath.Join(dirPath, data.Name))
	if err := d.pre(&path); err != nil {
		return err
	}

	// 大きすぎたらエラー
	if uint64(data.Size) > dropboxMaxSize {
		return fmt.Errorf("%dbyte データのサイズが大きすぎます。%dbyte以内におさめてください。", data.Size, dropboxMaxSize)
	}

	// commitInfoを作る。timeはutcniにして秒で丸める
	commitInfo := dbx.NewCommitInfo(path)
	commitInfo.ClientModified = TimeToDropbox(data.LastMod)
	commitInfo.Autorename = false
	commitInfo.Mode = &dbx.WriteMode{Tagged: dbxapi.Tagged{Tag: dbx.WriteModeOverwrite}}

	// 呼び出されるたびに次のチャンクをReaderとして返すやつ
	getNextChunk := func() io.Reader {
		chunk := io.LimitReader(data.Data, int64(dropboxChunkSize))
		return chunk
	}
	defer data.Data.Close()

	// ChunkSizeが150MNB以上と以下とで処理を分ける。
	// 150MB以上だと分割する必要があるので
	client := d.Client
	var err error
	if uint64(data.Size) < dropboxChunkSize {
		_, err = client.Upload(commitInfo, getNextChunk())
		if err != nil {
			err = fmt.Errorf("failed to upload %s at dropbox: %w", path, err)
			return err
		}
		return nil
	} // 最初のチャンク
	sarg := dbx.NewUploadSessionStartArg()
	res, err := client.UploadSessionStart(sarg, getNextChunk())
	if err != nil {
		err = fmt.Errorf("failed to upload session start %s at dropbox: %w", path, err)
		return err
	}

	// 最初、最後以外のチャンク
	uploaded := uint64(dropboxChunkSize)
	datasize := uint64(data.Size)
	for datasize-uploaded > dropboxChunkSize {
		c := dbx.NewUploadSessionCursor(res.SessionId, uploaded)
		aarg := dbx.NewUploadSessionAppendArg(c)

		err := client.UploadSessionAppendV2(aarg, getNextChunk())
		if err != nil {
			err = fmt.Errorf("failed to upload session append %s at dropbox: %w", path, err)
			return err
		}
		uploaded += dropboxChunkSize
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

// pathとそれの中身をすべて削除します。
func (d *dropbox) Delete(filepath string) error {
	dir, f := path.Split(filepath)
	filepath = path.Join(dir, f)

	if err := d.pre(&filepath); err != nil {
		return err
	}

	_, err := d.Client.DeleteV2(dbx.NewDeleteArg(filepath))
	if err != nil {
		err = fmt.Errorf("failed to delete %s: %w", filepath, err)
		return err
	}
	return nil
}

// ディレクトリを作成します。
func (d *dropbox) MkDir(filepath string) error {
	dir, f := path.Split(filepath)
	filepath = path.Join(dir, f)

	if err := d.pre(&filepath); err != nil {
		return err
	}

	_, err := d.Client.CreateFolderV2(dbx.NewCreateFolderArg(filepath))
	if err != nil {
		err = fmt.Errorf("failed to create folder %s at dropbox: %w", filepath, err)
		return err
	}
	return nil
}

// ストレージタイプを取得します。例えばdropboxならdropboxです
func (d *dropbox) Type() string {
	return "dropbox"
}

// ストレージ名を取得します。
func (d *dropbox) Name() string {
	return d.name
}

// このストレージを閉じます。
func (d *dropbox) Close() error {
	d.Client = nil
	return nil
}

// ディレクトリであればtrueをかえす
func (d *dropbox) isDir(filepath string) (bool, error) {
	dir, f := path.Split(filepath)
	filepath = path.Join(dir, f)

	// ディレクトリであるかどうかの判断
	arg := dbx.NewGetMetadataArg(filepath)
	metadata, err := d.Client.GetMetadata(arg)
	if err != nil {
		err = fmt.Errorf("failed to get %s metadata at dropbox: %w", filepath, err)
		return false, err
	}
	fileInfo, err := metadataToFileInfo(metadata)
	if err != nil {
		err = fmt.Errorf("failed to metadataToFileInfo %s: %w", filepath, err)
		return false, err
	}
	return fileInfo.IsDir, nil
}

// ディレクトリ内のメタデータをdropbox.metadataで取得します
func (d *dropbox) listFolder(dirpath string) (map[dbx.IsMetadata]interface{}, error) {
	dir, f := path.Split(dirpath)
	dirpath = path.Join(dir, f)

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

// dropbox.metadataをfileinfoに変換します
func metadataToFileInfo(metadata dbx.IsMetadata) (*FileInfo, error) {
	switch metadata.(type) {
	case *dbx.FolderMetadata:
		fo := metadata.(*dbx.FolderMetadata)
		return &FileInfo{
			IsDir: true,
			Name:  fo.Name,
			Path:  fo.PathDisplay,
		}, nil
	case *dbx.FileMetadata:
		fi := metadata.(*dbx.FileMetadata)
		return &FileInfo{
			IsDir:   false,
			Name:    fi.Name,
			Path:    fi.PathDisplay,
			Size:    int64(fi.Size),
			LastMod: fi.ClientModified,
		}, nil
	}
	err := fmt.Errorf("metadata is not folder and file. metadata=%s", metadata)
	return nil, err
}

/*
func (d *dropbox) Move(srcPath, destPath string) error {
	dir, f := path.Split(srcPath)
	srcPath = path.Join(dir, f)

	dir, f = path.Split(destPath)
	destPath = path.Join(dir, f)

	arg := dbx.NewRelocationArg(srcPath, destPath)
	_, err := d.Client.Move(arg)
	if err != nil {
		err = fmt.Errorf("failed to move files from %s to %s at dropbox: %w", srcPath, destPath, err)
		return err
	}
	return nil
}
*/

// ルートディレクトリは空文字で表現されるので予め解決しておくためのもの
func (d *dropbox) pre(path *string) error {
	*path = filepath.ToSlash(*path)
	// ルートディレクトリは空文字で表現する
	if *path == "/" {
		*path = ""
	}
	return nil
}

// $HOME/hbg_config_$name.yamlからtokenを読み取るか、なければ作成してユーザーに入力を求めます
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
		authorizeURL := authCfg.AuthCodeURL(
			uuid.New().String(),
			oauth2.SetAuthURLParam("response_type", "code"),
		)

		fmt.Printf("%s: %s の初期化を行います。\n下記のURLを開いてhbgを許可し、表示されたキーをこの画面に貼り付けてください。\n%s\n", "dropbox", name, authorizeURL)
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
