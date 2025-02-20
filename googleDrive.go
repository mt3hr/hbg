package hbg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mitchellh/go-homedir"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

var googleDriveMimeTypeFolder = "application/vnd.google-apps.folder"

type googleDrive struct {
	srv  *drive.Service
	name string
}

// NewGoogleDrive .
// googledriveを読み込みます。
// nameは任意の名前です。
// 初回起動時にコマンドライン入力を求められ、
// $HOME/hbg_config_$name.yamlにキーが保存され、以後楽に接続できるようになります。
func NewGoogleDrive(name string) (Storage, error) {
	srv, err := getGoogleDriveService(name)
	if err != nil {
		err = fmt.Errorf("load google drive failed %s. %w", name, err)
		return nil, err
	}
	return &googleDrive{
		srv:  srv,
		name: name,
	}, nil
}

// ディレクトリ内のファイルを列挙します。
func (g *googleDrive) List(filepath string) ([]*FileInfo, error) {
	fileInfos := []*FileInfo{}

	if filepath == "/" {
		files, err := g.listFiles("root")
		for _, file := range files {
			modTime := time.Time{}
			if file.ModifiedTime != "" {
				modTime, err = time.Parse(time.RFC3339, file.ModifiedTime)
				if err != nil {
					err = fmt.Errorf("time parse failed %s. %w", file.ModifiedTime, err)
					return nil, err
				}
			}

			fileInfo := &FileInfo{
				Path:  path.Join(filepath, file.Name),
				IsDir: file.MimeType == googleDriveMimeTypeFolder,

				Name:    file.Name,
				LastMod: modTime,
				Size:    file.Size,
			}
			fileInfos = append(fileInfos, fileInfo)
		}
		return fileInfos, nil
	}

	sepPath := strings.Split(filepath, "/")
	files, err := g.listFiles("root")
	if err != nil {
		err = fmt.Errorf("error at list files %s %s: %w", "root", g.Name(), err)
		return nil, err
	}

	for i := 0; i < len(sepPath); i++ {
		nextID := ""
		for _, file := range files {
			if sepPath[i] == file.Name && file.MimeType == googleDriveMimeTypeFolder {
				nextID = file.Id
				break
			}
		}
		if nextID != "" {
			files, err = g.listFiles(nextID)
		}
	}
	for _, file := range files {
		modTime := time.Time{}
		if file.ModifiedTime != "" {
			modTime, err = time.Parse(time.RFC3339, file.ModifiedTime)
			if err != nil {
				err = fmt.Errorf("time parse failed %s. %w", file.ModifiedTime, err)
				return nil, err
			}
		}

		fileInfo := &FileInfo{
			Path:  path.Join(filepath, file.Name),
			IsDir: file.MimeType == googleDriveMimeTypeFolder,

			Name:    file.Name,
			LastMod: modTime,
			Size:    file.Size,
		}
		fileInfos = append(fileInfos, fileInfo)
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

	dir := &drive.File{}
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

// ディレクトリの子ファイルを取得します。
func (g *googleDrive) listFiles(parentID string) ([]*drive.File, error) {
	files, err := g.srv.Files.List().Q(fmt.Sprintf("'%s' in parents and trashed=false", parentID)).PageSize(1000).Fields("nextPageToken, files(parents, id, name, kind, mimeType, modifiedTime, size)").Do()
	if files == nil {
		return nil, err
	}
	return files.Files, err
}

// ファイルパスからdrive.Fileオブジェクトを作成します。
func (g *googleDrive) getFileByPath(filepath string) (*drive.File, error) {
	d, f := path.Split(filepath)
	filepath = path.Join(d, f)

	dir, filename := path.Split(filepath)
	sepPath := strings.Split(dir, "/")

	if filepath == "/" {
		files, err := g.listFiles("root")
		if err != nil {
			err = fmt.Errorf("error at list files %s at %s: %w", "root", g.Name(), err)
			return nil, err
		}
		for _, file := range files {
			return file, nil
		}
	}

	files, err := g.listFiles("root")
	if err != nil {
		err = fmt.Errorf("error at list files %s at %s: %w", "root", g.Name(), err)
		return nil, err
	}

	for i := 0; i < len(sepPath); i++ {
		nextID := ""
		for _, file := range files {
			if sepPath[i] == file.Name {
				nextID = file.Id
				break
			}
		}
		if nextID != "" {
			files, err = g.listFiles(nextID)
		}
	}
	for _, file := range files {
		if file.Name != filename {
			continue
		}
		return file, nil
	}
	return nil, fmt.Errorf("%s: %s は見つかりませんでした。", g.Type(), filepath)
}

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config, name string) (*http.Client, error) {
	tokenFileName := fmt.Sprintf("hbg_token_%s_%s.json", "googledrive", name)
	home, err := homedir.Dir()
	if err != nil {
		err = fmt.Errorf("error at get user home directory: %w", err)
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		err = fmt.Errorf("error at get execute directory: %w", err)
		return nil, err
	}
	exe = filepath.Dir(exe)
	current := "."

	tok := &oauth2.Token{}
	for _, tokenDir := range []string{home, exe, current} {
		tokenFilePath := filepath.Join(tokenDir, tokenFileName)
		tokenFile := &os.File{}
		tokenFile, err = os.Open(tokenFilePath)
		if err == nil {
			defer tokenFile.Close()
			err = json.NewDecoder(tokenFile).Decode(tok)
			break
		}
	}
	if err != nil {
		authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
		fmt.Printf("%s: %s の初期化を行います。\n下記のURLを開いてhbgを許可し、表示されたキーをこの画面に貼り付けてください。\n%s\n", "googledrive", name, authURL)

		var authCode string
		if _, err := fmt.Scan(&authCode); err != nil {
			log.Fatalf("Unable to read authorization code %v", err)
		}

		tok, err := config.Exchange(context.TODO(), authCode)
		if err != nil {
			log.Fatalf("Unable to retrieve token from web %v", err)
		}

		tokenFilePath := filepath.Join(home, tokenFileName)
		err = saveToken(tokenFilePath, tok)
		if err != nil {
			err = fmt.Errorf("failed save token. %w", err)
			return nil, err
		}
	}
	return config.Client(context.Background(), tok), nil
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) error {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		err = fmt.Errorf("failed open file %s. %w", path, err)
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(token)
}

func getGoogleDriveService(name string) (*drive.Service, error) {
	b := []byte(`{"installed":{"client_id":"581224303741-8gad6gc0r1cdeam0r3rgmga140rgemr6.apps.googleusercontent.com","project_id":"hbg-go","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"_P9uv6G1xhQsToD9IJCsr3O7","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]}}`)
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client, err := getClient(config, name)
	if err != nil {
		err = fmt.Errorf("failed get client. %w", err)
		return nil, err
	}

	return drive.New(client)
}
