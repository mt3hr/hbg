package cmd

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bitbucket.org/mt3hr/hbg"
	glb "github.com/gobwas/glob"
	"github.com/jlaffaye/ftp"
	"github.com/spf13/cobra"
)

var (
	copyCmd = &cobra.Command{
		Run:   runCopy,
		Args:  cobra.ExactArgs(2),
		Use:   "copy srcStorage:srcPath destStorage:destDirPath",
		Short: "ストレージからストレージへとデータをコピーする",
		Long: `ストレージからストレージへとデータをコピーします。
最終更新時刻がupdate_duration未満のファイルのコピーはスキップされます。
対応しているストレージのタイプは以下です。
・local
・dropbox
・googledrive
・ftp
ftpをコピー先として使う場合、タイムスタンプの情報は消滅します。

GoogleDriveやDropboxを使わない場合は該当する行をコメントアウトするか除去してください。
GoogleDriveやDropboxは新たにnameを割り当てることで複数のアカウントを使うことができます。
name割当後の初回起動時には認証URLが出てくるので、コードを取得して貼り付けてください。`,
		Example: `使用例
hbg copy local:C:/Users/user/Desktop/test.txt dropbox:/hbg,
hbg copy dropbox:/hbg/test.txt local:/home/user/デスクトップ
hbg copy -w 10 local:C:/hoge local:C:/fuga


設定ファイルの例
local:
  name: local
dropbox:
- name: dropbox
googledrive: 
- name: googledrive
ftp: 
- name: ftp
  address: localhost
  username: anonymous
  password: password
`,
		PreRun: func(_ *cobra.Command, args []string) {
			srcInfo, destInfo := args[0], args[1]

			// コロンで区切って、前がstorageタイプ、後がpath
			srcSplit := strings.SplitN(srcInfo, ":", 2)
			copyOpt.srcStorage = srcSplit[0]
			copyOpt.srcPath = srcSplit[1]

			destSplit := strings.SplitN(destInfo, ":", 2)
			copyOpt.destStorage = destSplit[0]
			copyOpt.destDirPath = destSplit[1]
		},
	}

	copyOpt = &struct {
		srcStorage  string
		srcPath     string
		destStorage string
		destDirPath string

		worker         int
		updateDuration time.Duration
		ignore         []string
	}{}
)

func init() {
	copyFs := copyCmd.Flags()
	copyFs.StringArrayVarP(&copyOpt.ignore, "ignore", "i", []string{".nomedia", "desktop.ini", "thumbnails", ".thumbnails", "Thumbs.db"}, "無視するファイル")
	copyFs.DurationVar(&copyOpt.updateDuration, "update_duration", time.Duration(time.Second), "更新されたとみなす期間")
	copyFs.IntVarP(&copyOpt.worker, "worker", "w", 1, "同時処理数")
}

func runCopy(_ *cobra.Command, _ []string) {
	var srcStorage, destStorage hbg.Storage
	storages, err := storageMapFromConfig(cfg)
	if err != nil {
		err = fmt.Errorf("failed to load storagemap from config: %w", err)
		if err != nil {
			log.Fatal(err)
		}
	}
	srcStorage, exist := storages[copyOpt.srcStorage]
	if !exist {
		err = fmt.Errorf("not found storage '%s'", copyOpt.srcStorage)
		log.Fatal(err)
	}
	destStorage, exist = storages[copyOpt.destStorage]
	if !exist {
		err = fmt.Errorf("not found storage '%s'", copyOpt.destStorage)
		log.Fatal(err)
	}

	err = copy(srcStorage, destStorage, copyOpt.srcPath, copyOpt.destDirPath, copyOpt.updateDuration, copyOpt.ignore, copyOpt.worker)
	if err != nil {
		err = fmt.Errorf("failed to copy file from %s:%s to %s:%s: %w", srcStorage.Type(), copyOpt.srcPath, destStorage.Type(), copyOpt.destDirPath, err)
		log.Fatal(err)
	}
}

func storageMapFromConfig(c *Cfg) (map[string]hbg.Storage, error) {
	storages := map[string]hbg.Storage{}

	// localの読み込み
	storages[c.Local.Name] = &hbg.LocalFileSystem{}

	// dropboxの読み込み
	for _, dbxCfg := range c.Dropbox {
		dropbox, err := hbg.NewDropbox(dbxCfg.Name)
		if err != nil {
			err = fmt.Errorf("failed load dropbox %s. %w", dbxCfg.Name, err)
			return nil, err
		}
		_, exist := storages[dbxCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of dropbox storage '%s'", dbxCfg.Name)
			return nil, err
		}
		storages[dbxCfg.Name] = dropbox
	}

	for _, gdvCfg := range c.GoogleDrive {
		googleDrive, err := hbg.NewGoogleDrive(gdvCfg.Name)
		if err != nil {
			err = fmt.Errorf("failed load google drive %s. %w", gdvCfg.Name, err)
			return nil, err
		}
		_, exist := storages[gdvCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of google drive storage '%s'", gdvCfg.Name)
			return nil, err
		}
		storages[gdvCfg.Name] = googleDrive
	}

	// ftpの読み込み
	// プログラム終了時まで閉じられることがない問題
	for _, ftpCfg := range c.FTP {
		conn, err := ftp.Connect(ftpCfg.Address)
		if err != nil {
			err = fmt.Errorf("failed to connect to ftp server %s: %w", ftpCfg.Address, err)
			return nil, err
		}
		if ftpCfg.UserName != "" || ftpCfg.Password != "" {
			conn.Login(ftpCfg.UserName, ftpCfg.Password)
		}

		ftp := &hbg.FTP{Conn: conn}
		_, exist := storages[ftpCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of ftp storage '%s'", ftpCfg.Name)
			return nil, err
		}
		storages[ftpCfg.Name] = ftp
	}

	return storages, nil
}

func glob(files map[*hbg.FileInfo]interface{}, pattern string) (map[*hbg.FileInfo]interface{}, error) {
	fileInfos := map[*hbg.FileInfo]interface{}{}

	g := glb.MustCompile(filepath.ToSlash(pattern))
	for file := range files {
		if g.Match(filepath.ToSlash(file.Path)) {
			fileInfos[file] = struct{}{}
		}
	}
	return fileInfos, nil
}

func copy(srcStorage, destStorage hbg.Storage, srcPath, destDirPath string, updateDuration time.Duration, ignores []string, worker int) error {
	return cp(srcStorage, destStorage, srcPath, destDirPath, updateDuration, ignores, worker, nil, nil)
}

// destFileInfosは、移動先フォルダをListしたもの。
// srcFileInfosは、移動元フォルダをListしたもの。
func cp(srcStorage, destStorage hbg.Storage, srcPath, destDirPath string, updateDuration time.Duration, ignores []string, worker int, srcFileInfos map[*hbg.FileInfo]interface{}, destFileInfos map[*hbg.FileInfo]interface{}) error {
	// どちらもディレクトリの場合
	var err error

	if srcFileInfos == nil {
		parentDir := filepath.Dir(srcPath)
		parentDir = filepath.ToSlash(parentDir)
		srcFiles, err := srcStorage.List(parentDir)
		if err != nil {
			err = fmt.Errorf("failed list %s at %s. %w", parentDir, srcStorage.Type())
			return err
		}
		srcFileInfos, err = glob(srcFiles, srcPath)
	}
	if err != nil {
		err = fmt.Errorf("failed glob %s. %w", srcPath, err)
		return err
	}

	if destFileInfos == nil {
		destFileInfos, err = destStorage.List(destDirPath)
		// ディレクトリがないとエラーが飛びえるので
		if err != nil {
			time.Sleep(1 * time.Second)
			err = destStorage.MkDir(destDirPath)
			if err != nil {
				time.Sleep(1 * time.Second)
				err = destStorage.MkDir(destDirPath)
				if err != nil {
					err = fmt.Errorf("failed to create directory %s:%s: %w", destStorage.Type(), destDirPath, err)
					return err
				}
			}
			destFileInfos, err = destStorage.List(destDirPath)
			if err != nil {
				err = fmt.Errorf("failed to list directory %s:%s: %w", destStorage.Type(), destDirPath, err)
				return err
			}
		}
	}

	// ワーカー
	q := make(chan *copyFileArg, worker)
	wg := &sync.WaitGroup{}
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go copyFileWorker(q, wg)
	}

Loop:
	for srcFileInfo := range srcFileInfos {
		// 無視するファイル名だったら無視
		for _, ignore := range ignores {
			if srcFileInfo.Name == ignore {
				continue Loop
			}
		}

		// ディレクトリだったら再帰的に
		if srcFileInfo.IsDir {
			files, err := srcStorage.List(srcFileInfo.Path)
			if err != nil {
				err = fmt.Errorf("failed list %s at %s. %w", srcFileInfo.Name, srcStorage.Type(), err)
				return err
			}

			dir, destDirPath := "", destDirPath
			for file := range files {
				dir = filepath.ToSlash(filepath.Dir(file.Path))
			}
			if dir == "" {
				destDirPath = filepath.ToSlash(destDirPath)
			} else {
				destDirPath = filepath.ToSlash(filepath.Join(destDirPath, filepath.Base(dir)))
			}
			destFileInfos, err := destStorage.List(destDirPath)
			// ディレクトリがないとエラーが飛びえるので
			if err != nil {
				time.Sleep(time.Second * 1) // 叩きすぎると怒られるので
				err = destStorage.MkDir(destDirPath)
				if err != nil {
					time.Sleep(time.Second * 1) // 叩きすぎると怒られるので
					err = destStorage.MkDir(destDirPath)
					if err != nil {
						err = fmt.Errorf("failed to create directory %s:%s: %w", destStorage.Type(), destDirPath, err)
						return err
					}
				}
				destFileInfos, err = destStorage.List(destDirPath)
				if err != nil {
					err = fmt.Errorf("failed to list directory %s:%s: %w", destStorage.Type(), destDirPath, err)
					return err
				}
			}

			srcParentDir := ""
			for file := range files {
				srcParentDir = filepath.ToSlash(filepath.Dir(file.Path))
			}
			srcFiles, err := srcStorage.List(srcParentDir)
			if err != nil {
				err = fmt.Errorf("failed list %s at %s. %w", srcParentDir, srcStorage.Type())
				return err
			}

			for file := range files {
				if file.IsDir {
					err = cp(srcStorage, destStorage, filepath.ToSlash(file.Path), destDirPath, updateDuration, ignores, worker, nil, nil)
					if err != nil {
						return err
					}
				} else {
					matchSrcFiles := map[*hbg.FileInfo]interface{}{}
					for f := range srcFiles {
						if file.Name == f.Name {
							matchSrcFiles[f] = struct{}{}
						}
					}

					err = cp(srcStorage, destStorage, filepath.ToSlash(file.Path), destDirPath, updateDuration, ignores, worker, matchSrcFiles, destFileInfos)
					if err != nil {
						return err
					}
				}
			}
			continue Loop
		}

		// ファイルで、
		// 最終更新時刻の差がそれ未満かつ、ファイルサイズが同一だったらスキップ
		for destFileInfo := range destFileInfos {
			if srcFileInfo.Name == destFileInfo.Name {
				srcTimeUTC := srcFileInfo.LastMod.UTC()
				destTimeUTC := destFileInfo.LastMod.UTC()
				duration := srcTimeUTC.Sub(destTimeUTC)

				d := int64(duration)
				if d < 0 {
					d *= int64(-1)
				}
				if d <= int64(updateDuration) &&
					srcFileInfo.Size == destFileInfo.Size {
					continue Loop
				}
			}
		}

		// コピー
		q <- &copyFileArg{
			srcStorage:  srcStorage,
			destStorage: destStorage,
			srcFilePath: srcFileInfo.Path,
			destDirPath: destDirPath,
		}
	}

	close(q)
	wg.Wait()
	return nil
}

func copyFileWorker(q <-chan *copyFileArg, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		arg, ok := <-q
		if !ok {
			return
		}
		err := copyFile(arg.srcStorage, arg.destStorage, arg.srcFilePath, arg.destDirPath)
		if err != nil {
			err = fmt.Errorf("failed to copy file from %s:%s to %s:%s: %w", arg.srcStorage.Type(), arg.srcFilePath, arg.destStorage.Type(), arg.destDirPath, err)
			log.Printf("%s\n", err)
		}
	}
}

type copyFileArg struct {
	srcStorage  hbg.Storage
	destStorage hbg.Storage
	srcFilePath string
	destDirPath string
}

func copyFile(srcStorage, destStorage hbg.Storage, srcFilePath, destDirPath string) error {
	fmt.Printf("copy %s:%s -> %s:%s\n", srcStorage.Type(), srcFilePath, destStorage.Type(), destDirPath)
	file, err := srcStorage.Get(srcFilePath)
	if err != nil {
		err = fmt.Errorf("failed to get %s:%s : %w", srcStorage.Type(), srcFilePath, err)
		return err
	}
	defer file.Data.Close()
	err = destStorage.Push(destDirPath, file)
	if err != nil {
		err = fmt.Errorf("failed to push from %s:%s to %s:%s : %w", srcStorage.Type(), srcFilePath, destStorage.Type(), destDirPath, err)
		return err
	}
	return nil
}
