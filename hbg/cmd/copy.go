package cmd

import (
	"fmt"
	"log"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bitbucket.org/mt3hr/hbg"
	"github.com/spf13/cobra"
	errors "golang.org/x/xerrors"

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
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
	次に例を示します。
	hbg copy local:C:\Users\user\Desktop\test.txt dropbox:/hbg`,
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

	// copyOptの初期化
	copyCmd.PreRun = func(_ *cobra.Command, args []string) {
		srcInfo, destInfo := args[0], args[1]

		// コロンで区切って、前がstorageタイプ、後がpath
		srcSplit := strings.SplitN(srcInfo, ":", 2)
		copyOpt.srcStorage = srcSplit[0]
		copyOpt.srcPath = srcSplit[1]

		destSplit := strings.SplitN(destInfo, ":", 2)
		copyOpt.destStorage = destSplit[0]
		copyOpt.destDirPath = destSplit[1]

		// copyOpt.srcStorageとcopyOpt.destStorageの整合性チェック
		for _, storage := range []string{copyOpt.srcStorage, copyOpt.destStorage} {
			switch storage {
			case st_local, st_dropbox:
			default:
				err := fmt.Errorf("unknown storage type. %s", storage)
				log.Fatal(err)
			}
		}
	}
}

// storageType
const (
	st_local   = "local"
	st_dropbox = "dropbox"
)

func runCopy(_ *cobra.Command, _ []string) {
	var srcStorage, destStorage hbg.Storage
	switch copyOpt.srcStorage {
	case st_local:
		srcStorage = &hbg.LocalFileSystem{}
	case st_dropbox:
		client := dbx.New(dropbox.Config{Token: cfg.DropboxToken})
		srcStorage = &hbg.Dropbox{client}
	}
	switch copyOpt.destStorage {
	case st_local:
		destStorage = &hbg.LocalFileSystem{}
	case st_dropbox:
		client := dbx.New(dropbox.Config{Token: cfg.DropboxToken})
		destStorage = &hbg.Dropbox{client}
	}

	err := copy(srcStorage, destStorage, copyOpt.srcPath, copyOpt.destDirPath, copyOpt.updateDuration, copyOpt.ignore, copyOpt.worker)
	if err != nil {
		err = errors.Errorf("failed to copy file from %s:%s to %s:%s: %w", srcStorage, copyOpt.srcPath, destStorage, copyOpt.destDirPath, err)
		log.Fatal(err)
	}
}
func copy(srcStorage, destStorage hbg.Storage, srcPath, destDirPath string, updateDuration time.Duration, ignores []string, worker int) error {
	// どちらもディレクトリの場合
	srcFileInfos, err := srcStorage.List(srcPath)
	if err != nil {
		err = errors.Errorf("failed to list directory %s:%s: %w", srcStorage, srcPath, err)
		return err
	}

	destFileInfos, err := destStorage.List(destDirPath)
	// ディレクトリがないとエラーが飛びえるので
	if err != nil {
		err = destStorage.MkDir(destDirPath)
		if err != nil {
			err = errors.Errorf("failed to create directory %s:%s: %w", destStorage, destDirPath, err)
			return err
		}
		destFileInfos, err = destStorage.List(destDirPath)
		if err != nil {
			err = errors.Errorf("failed to list directory %s:%s: %w", destStorage, destDirPath, err)
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
			err = errors.Errorf("failed to copy file from %s:%s to %s:%s: %w", arg.srcStorage, arg.srcFilePath, arg.destStorage, arg.destDirPath, err) //TODO srcStorage, destStorageは文字列じゃないよ
			log.Printf("%s\n", err)

			// 失敗したらコピー先ファイルを削除する
			err = func() error {
				srcFile, err := arg.srcStorage.Stat(arg.srcFilePath)
				if err != nil {
					err = errors.Errorf("failed to get stat %s from %s: %w", arg.srcFilePath, arg.srcStorage, err)
					return err
				}
				destFilePath := path.Join(arg.destDirPath, srcFile.Name)

				err = arg.destStorage.Delete(destFilePath)
				if err != nil {
					err = errors.Errorf("failed to delete to %s:%s: %w", arg.destStorage, destFilePath, err)
					return err
				}
				return nil
			}()
			if err != nil {
				err = errors.Errorf("failed to delete copy failed file: %w", err)
				log.Printf("%s\n", err)
			}
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
		err = errors.Errorf("failed to get %s:%s : %w", srcStorage, srcFilePath, err)
		return err
	}
	defer file.Data.Close()
	err = destStorage.Push(destDirPath, file)
	if err != nil {
		err = errors.Errorf("failed to push from %s:%s to %s:%s : %w", srcStorage, srcFilePath, destStorage, destDirPath, err)
		return err
	}
	return nil
}
