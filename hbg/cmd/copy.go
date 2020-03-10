package cmd

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bitbucket.org/mt3hr/hbg"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/jlaffaye/ftp"
	"github.com/spf13/cobra"
)

var (
	copyCmd = &cobra.Command{
		Run:   runCopy,
		Args:  cobra.ExactArgs(2),
		Use:   "copy srcStorage:srcPath destStorage:destDirPath",
		Short: "ストレージからストレージへとデータをコピーする",
		Long: `
ストレージからストレージへとデータをコピーします。
最終更新時刻がupdate_duration未満のファイルのコピーはスキップされます。
対応しているストレージのタイプは以下です。
・local
・dropbox
・ftp
ftpをコピー先として使う場合、タイムスタンプの情報は消滅します。`,
		Example: `使用例
hbg copy local:C:/Users/user/Desktop/test.txt dropbox:/hbg,
hbg copy dropbox:/hbg/test.txt local:/home/user/デスクトップ
hbg copy -w 10 local:C:/hoge local:C:/fuga

設定ファイルの例
local:
  name: local
dropbox:
- name: dropbox
  token: hogefugapiyo1234567890
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
		client := dbx.New(dropbox.Config{Token: dbxCfg.Token})
		dropbox := &hbg.Dropbox{client}
		_, exist := storages[dbxCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of dropbox storage '%s'", dbxCfg.Name)
			return nil, err
		}
		storages[dbxCfg.Name] = dropbox
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

func copy(srcStorage, destStorage hbg.Storage, srcPath, destDirPath string, updateDuration time.Duration, ignores []string, worker int) error {
	// どちらもディレクトリの場合
	srcFileInfos, err := srcStorage.List(srcPath)
	if err != nil {
		err = fmt.Errorf("failed to list directory %s:%s: %w", srcStorage.Type(), srcPath, err)
		return err
	}

	destFileInfos, err := destStorage.List(destDirPath)
	// ディレクトリがないとエラーが飛びえるので
	if err != nil {
		err = destStorage.MkDir(destDirPath)
		if err != nil {
			err = fmt.Errorf("failed to create directory %s:%s: %w", destStorage.Type(), destDirPath, err)
			return err
		}
		destFileInfos, err = destStorage.List(destDirPath)
		if err != nil {
			err = fmt.Errorf("failed to list directory %s:%s: %w", destStorage.Type(), destDirPath, err)
			return err
		}
	}

	// ワーカー
	q := make(chan *copyFileArg, worker)
	wg := &sync.WaitGroup{}
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go copyFileWorker(q, wg)
	}

	for srcFileInfo := range srcFileInfos {
		skip := false
		// 無視するファイル名だったら無視
		for _, ignore := range ignores {
			if srcFileInfo.Name == ignore {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		// ディレクトリだったら再帰的に
		if srcFileInfo.IsDir {
			childDestDir := filepath.ToSlash(filepath.Join(destDirPath, srcFileInfo.Name))
			err := copy(srcStorage, destStorage, srcFileInfo.Path, childDestDir, updateDuration, ignores, worker)
			if err != nil {
				return err
			}
			continue
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
					skip = true
					break
				}
			}
		}
		if skip {
			continue
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
			/*
				// 失敗したらコピー先ファイルを削除する
				err = func() error {
					srcFile, err := arg.srcStorage.Stat(arg.srcFilePath)
					if err != nil {
						err = fmt.Errorf("failed to get stat %s from %s: %w", arg.srcFilePath, arg.srcStorage.Type(), err)
						return err
					}
					destFilePath := path.Join(arg.destDirPath, srcFile.Name)

					err = arg.destStorage.Delete(destFilePath)
					if err != nil {
						err = fmt.Errorf("failed to delete to %s:%s: %w", arg.destStorage.Type(), destFilePath, err)
						return err
					}
					return nil
				}()
				if err != nil {
					err = fmt.Errorf("failed to delete copy failed file: %w", err)
					log.Printf("%s\n", err)
				}
			*/
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
	fmt.Printf("copy %s:%s > %s:%s\n", srcStorage.Type(), srcFilePath, destStorage.Type(), destDirPath)
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
