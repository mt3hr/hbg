package hbg

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	drive "google.golang.org/api/drive/v3"
)

var googleDriveMimeTypeFolder = "application/vnd.google-apps.folder"

type googleDrive struct {
	srv  *drive.Service
	name string
}

// GoogleDriveConfig は Google Drive ストレージの設定です。
type GoogleDriveConfig struct {
	// Name は設定ファイルで付けた名前です。コマンドで "名前:パス" として使います。
	Name string
	// ClientID と ClientSecret は OAuth クライアントの識別情報です。
	// 空の場合は環境変数やビルド時に埋め込まれた値が使われます。
	ClientID     string
	ClientSecret string
}

// NewGoogleDrive .
// googledriveを読み込みます。
//
// 保存済みのトークンを使って接続します。トークンがない場合はエラーを返すので、
// hbg auth login <名前> で認証してください。
//
// 以前はここで対話的な認証を始めていましたが、使わないストレージの
// 認証まで走ってしまううえ、コピーの途中で突然ブラウザが開くことになるため、
// 認証は hbg auth login に分離しています。
func NewGoogleDrive(cfg GoogleDriveConfig) (Storage, error) {
	srv, err := newGoogleDriveService(cfg)
	if err != nil {
		return nil, fmt.Errorf("load google drive failed %s. %w", cfg.Name, err)
	}
	return &googleDrive{
		srv:  srv,
		name: cfg.Name,
	}, nil
}

// ディレクトリ内のファイルを列挙します。
//
// 以前はルート直下の列挙で listFiles のエラーを検査しておらず、
// API が失敗しても「空のディレクトリ」として正常終了していました。
// コピー先が空だと誤認して不要な転送を行うため、必ず検査します。
func (g *googleDrive) List(filepath string) ([]*FileInfo, error) {
	dirID, err := g.resolveDirID(filepath)
	if err != nil {
		return nil, err
	}

	files, err := g.listFiles(dirID)
	if err != nil {
		return nil, err
	}

	fileInfos := make([]*FileInfo, 0, len(files))
	for _, file := range files {
		modTime := time.Time{}
		if file.ModifiedTime != "" {
			modTime, err = time.Parse(time.RFC3339, file.ModifiedTime)
			if err != nil {
				return nil, fmt.Errorf("time parse failed %s. %w", file.ModifiedTime, err)
			}
		}

		fileInfos = append(fileInfos, &FileInfo{
			Path:  path.Join(filepath, file.Name),
			IsDir: file.MimeType == googleDriveMimeTypeFolder,

			Name:    file.Name,
			LastMod: modTime,
			Size:    file.Size,
		})
	}
	return fileInfos, nil
}

// ファイルのメタデータを取得します。存在しなかった場合はエラーを返します。
func (g *googleDrive) Stat(filepath string) (*FileInfo, error) {
	if filepath == "/" {
		return &FileInfo{
			IsDir: true,
			Name:  "/",
			Path:  "/",
		}, nil
	}

	file, err := g.getFileByPath(filepath)
	if err != nil {
		err = fmt.Errorf("failed get file by path %s. %w", filepath, err)
		return nil, err
	}

	modTime := time.Time{}
	if file.ModifiedTime != "" {
		modTime, err = time.Parse(time.RFC3339, file.ModifiedTime)
		if err != nil {
			err = fmt.Errorf("time parse failed %s. %w", file.ModifiedTime, err)
			return nil, err
		}
	}

	fileInfo := &FileInfo{
		Path:  filepath,
		IsDir: file.MimeType == googleDriveMimeTypeFolder,

		Name:    file.Name,
		LastMod: modTime,
		Size:    file.Size,
	}
	return fileInfo, nil
}

// ファイルを取得します。存在しなかった場合はエラーを返します。かならずFile.Data.Close()してください。
func (g *googleDrive) Get(path string) (*File, error) {
	file, err := g.getFileByPath(path)
	if err != nil {
		err = fmt.Errorf("failed get file by path %s. %w", path, err)
		return nil, err
	}

	modTime := time.Time{}
	if file.ModifiedTime != "" {
		modTime, err = time.Parse(time.RFC3339, file.ModifiedTime)
		if err != nil {
			err = fmt.Errorf("time parse failed %s. %w", file.ModifiedTime, err)
			return nil, err
		}
	}

	res, err := g.srv.Files.Get(file.Id).Fields("files(parents, id, name, kind, mimeType, modifiedTime)").Download()
	if err != nil {
		err = fmt.Errorf("failed download %s google drive. %w", path, err)
		return nil, err
	}
	return &File{
		Data:    res.Body,
		Name:    file.Name,
		LastMod: modTime,
		Size:    file.Size,
	}, nil
}

// 親ディレクトリを作成し、ファイルを作成します。
// すでにファイルが存在する場合は上書きします。
func (g *googleDrive) Push(dirPath string, data *File) error {
	d, f := path.Split(dirPath)
	dirPath = path.Join(d, f)

	file := &drive.File{}
	if dirPath == "/" {
		file = &drive.File{
			Name:         data.Name,
			ModifiedTime: data.LastMod.Format(time.RFC3339),
		}
	} else {
		dir, err := g.getFileByPath(dirPath)
		if err != nil {
			err = g.MkDir(dirPath)
			if err != nil {
				err = fmt.Errorf("error at mkdir %s at %s: %w", dirPath, g.Name(), err)
				return err
			}
			dir, err = g.getFileByPath(dirPath)
			if err != nil {
				err = fmt.Errorf("error at get file by path %s at %s: %w", dirPath, g.Name(), err)
				return err
			}
		}

		file = &drive.File{
			Name:         data.Name,
			ModifiedTime: data.LastMod.Format(time.RFC3339),
			Parents:      []string{dir.Id},
		}
	}
	existDriveFile, err := g.getFileByPath(path.Join(dirPath, data.Name))
	exist := err == nil
	if !exist {
		_, err = g.srv.Files.Create(file).Media(data.Data).Do()
		if err != nil {
			err = fmt.Errorf("error at create %s at %s. %w", path.Join(dirPath, data.Name), g.Name(), err)
			return err
		}
	} else {
		file.Parents = nil
		_, err = g.srv.Files.Update(existDriveFile.Id, file).Media(data.Data).Do()
		if err != nil {
			err = fmt.Errorf("error at update %s at %s. %w", path.Join(dirPath, data.Name), g.Name(), err)
			return err
		}
	}

	return nil
}

// pathとそれの中身をすべて削除します。
func (g *googleDrive) Delete(path string) error {
	file, err := g.getFileByPath(path)
	if err != nil {
		err = fmt.Errorf("error at get file by path %s at %s. %w", path, g.Name(), err)
		return err
	}
	return g.srv.Files.Delete(file.Id).Do()
}

// ディレクトリを作成します。
func (g *googleDrive) MkDir(dirPath string) error {
	parentDirName, dirName := path.Split(dirPath)
	if dirName == "" {
		parentDirName, dirName = path.Split(parentDirName)
	}

	var dir *drive.File
	if parentDirName == "/" {
		dir = &drive.File{
			Name:     dirName,
			MimeType: "application/vnd.google-apps.folder",
		}
	} else {
		parentDir, err := g.getFileByPath(parentDirName)
		if err != nil {
			err := g.MkDir(path.Dir(parentDirName))
			if err != nil {
				return err
			}
			parentDir, err = g.getFileByPath(path.Dir(parentDirName))
			if err != nil {
				err = fmt.Errorf("failed get file by path %s. %w", parentDirName, err)
				return err
			}
		}
		dir = &drive.File{
			Name:     dirName,
			Parents:  []string{parentDir.Id},
			MimeType: "application/vnd.google-apps.folder",
		}
	}

	_, err := g.srv.Files.Create(dir).Do()
	if err != nil {
		err = fmt.Errorf("failed mkdir %s. %w", dirPath, err)
		return err
	}
	return nil
}

// ストレージタイプを取得します。例えばdropboxならdropboxです
func (g *googleDrive) Type() string {
	return "googledrive"
}

// ストレージ名を取得します。
func (g *googleDrive) Name() string {
	return g.name
}

// このストレージを閉じます。
func (g *googleDrive) Close() error {
	g.srv = nil
	return nil
}

// ディレクトリの子ファイルをすべて取得します。
//
// Files.List が一度に返す件数には上限があり、続きは nextPageToken を
// 辿って取得する必要があります。以前は nextPageToken を Fields に
// 指定しながら follow しておらず、子が1000件を超えるディレクトリで
// 超過分がエラーも警告もなく欠落していました。
// 同期ツールとしては「コピーされないファイルが出る」ことになるため、
// Pages ですべてのページを走査します。
func (g *googleDrive) listFiles(parentID string) ([]*drive.File, error) {
	files := []*drive.File{}

	call := g.srv.Files.List().
		Q(fmt.Sprintf("'%s' in parents and trashed=false", parentID)).
		PageSize(1000).
		Fields("nextPageToken, files(parents, id, name, kind, mimeType, modifiedTime, size)")

	err := call.Pages(context.Background(), func(page *drive.FileList) error {
		files = append(files, page.Files...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error at list files in %s at %s: %w", parentID, g.Name(), err)
	}
	return files, nil
}

// resolveDirID は、スラッシュ区切りのディレクトリパスを
// Google Drive のフォルダIDに解決します。ルートは "root" です。
//
// 以前は、途中のセグメントが見つからない場合にそれを黙って読み飛ばし、
// ひとつ上の階層の内容をそのまま結果として返していました。
// 存在しないディレクトリを指定すると別のディレクトリの内容が
// 正しい結果として返るため、コピーや削除の対象を取り違える恐れがあります。
// ここでは見つからない時点で明確に失敗させます。
func (g *googleDrive) resolveDirID(dir string) (string, error) {
	id := "root"
	for _, segment := range strings.Split(dir, "/") {
		if segment == "" || segment == "." {
			continue
		}

		files, err := g.listFiles(id)
		if err != nil {
			return "", err
		}

		nextID := ""
		for _, file := range files {
			// 途中のセグメントはフォルダでなければならない。
			// これを見ないと、ディレクトリと同名のファイルが
			// 解決先を横取りしてしまう。
			if file.Name == segment && file.MimeType == googleDriveMimeTypeFolder {
				nextID = file.Id
				break
			}
		}
		if nextID == "" {
			return "", fmt.Errorf("%s: ディレクトリ %s が見つかりませんでした（%s が存在しません）", g.Type(), dir, segment)
		}
		id = nextID
	}
	return id, nil
}

// ファイルパスからdrive.Fileオブジェクトを取得します。
// ファイルでもディレクトリでも取得できます。
func (g *googleDrive) getFileByPath(filepath string) (*drive.File, error) {
	d, f := path.Split(filepath)
	filepath = path.Join(d, f)

	dir, filename := path.Split(filepath)
	if filename == "" {
		return nil, fmt.Errorf("%s: ルートディレクトリはファイルとして取得できません", g.Type())
	}

	dirID, err := g.resolveDirID(dir)
	if err != nil {
		return nil, err
	}

	files, err := g.listFiles(dirID)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.Name == filename {
			return file, nil
		}
	}
	return nil, fmt.Errorf("%s: %s は見つかりませんでした。", g.Type(), filepath)
}
