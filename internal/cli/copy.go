package cli

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	glb "github.com/gobwas/glob"
	"github.com/mt3hr/hbg"
	"github.com/mt3hr/hbg/internal/hbglog"
	"github.com/spf13/cobra"
)

var (
	copyCmd = &cobra.Command{
		Aliases: []string{"cp"},
		RunE:    runCopy,
		Args:    cobra.ExactArgs(2),
		Use:     "copy srcStorage:srcPath destStorage:destDirPath",
		Short:   "ストレージからストレージへとデータをコピーする",
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
hbg copy local:C:/hoge/test.txt dropbox:/hbg
hbg copy dropbox:/hbg/test.txt local:/home/user/documents
hbg copy -w 10 local:C:/hoge local:C:/fuga


設定ファイルの例
DefaultWorker: 2
local:
  name: local
dropbox:
- name: dropbox
googledrive:
- name: googledrive
`,
		PreRunE: func(_ *cobra.Command, args []string) error {
			srcInfo, destInfo := args[0], args[1]

			// コロンで区切って、前がstorageタイプ、後がpath
			srcSplit := strings.SplitN(srcInfo, ":", 2)
			if len(srcSplit) < 2 {
				return withExitCode(ExitUsage, fmt.Errorf("srcpathの記述が変です: %q（storage:path の形式で指定してください）", srcInfo))
			}
			copyOpt.srcStorage = srcSplit[0]
			copyOpt.srcPath = srcSplit[1]

			destSplit := strings.SplitN(destInfo, ":", 2)
			if len(destSplit) < 2 {
				return withExitCode(ExitUsage, fmt.Errorf("destpathの記述が変です: %q（storage:path の形式で指定してください）", destInfo))
			}
			copyOpt.destStorage = destSplit[0]
			copyOpt.destDirPath = destSplit[1]

			if copyOpt.worker == 0 {
				copyOpt.worker = config.DefaultWorker
			}
			// 設定ファイルに DefaultWorker がない場合、以前はここが 0 のままとなり、
			// 容量0のチャネルとワーカー0個で永久にブロックしていた。
			if copyOpt.worker < 1 {
				copyOpt.worker = defaultWorker
			}
			return nil
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

// defaultIgnores は既定で無視するファイル名です。
// CLI の --ignore の既定値であり、シェルの cp でも使われます。
var defaultIgnores = []string{
	".nomedia",
	"desktop.ini",
	"thumbnails",
	".thumbnails",
	"Thumbs.db",
	".DS_Store",
	".localized",
}

func init() {
	copyFs := copyCmd.Flags()
	copyFs.StringArrayVarP(&copyOpt.ignore, "ignore", "i", defaultIgnores, "無視するファイル")
	copyFs.DurationVar(&copyOpt.updateDuration, "update_duration", time.Second, "更新されたとみなす期間")
	copyFs.IntVarP(&copyOpt.worker, "worker", "w", 0, "同時処理数。0だとconfigファイルの値で動きます。")
}

func runCopy(_ *cobra.Command, _ []string) error {
	storages, err := storageMapFromConfig(config)
	if err != nil {
		return fmt.Errorf("error at load storagemap from config: %w", err)
	}
	srcStorage, exist := storages[copyOpt.srcStorage]
	if !exist {
		return withExitCode(ExitUsage, fmt.Errorf("not found storage '%s'", copyOpt.srcStorage))
	}
	destStorage, exist := storages[copyOpt.destStorage]
	if !exist {
		return withExitCode(ExitUsage, fmt.Errorf("not found storage '%s'", copyOpt.destStorage))
	}

	result, err := copyTree(srcStorage, destStorage, copyOpt.srcPath, copyOpt.destDirPath, copyOpt.updateDuration, copyOpt.ignore, copyOpt.worker)
	if err != nil {
		return fmt.Errorf("error at copy file from %s:%s to %s:%s: %w", srcStorage.Type(), copyOpt.srcPath, destStorage.Type(), copyOpt.destDirPath, err)
	}

	result.writeSummary(os.Stdout)
	if result.Failed > 0 {
		return withExitCode(ExitTransferFailed,
			fmt.Errorf("%d件のファイルのコピーに失敗しました", result.Failed))
	}
	return nil
}

func glob(files []*hbg.FileInfo, pattern string) ([]*hbg.FileInfo, error) {
	fileInfos := []*hbg.FileInfo{}

	g := glb.MustCompile(filepath.ToSlash(pattern))
	for _, file := range files {
		if g.Match(filepath.ToSlash(file.Path)) {
			fileInfos = append(fileInfos, file)
		}
	}
	return fileInfos, nil
}

// サマリで表示する失敗の最大件数。
// 大量に失敗した場合に画面を埋め尽くさないための上限です。
const maxReportedErrors = 20

// copyResult は転送の集計結果です。
type copyResult struct {
	Transferred int
	Failed      int
	Elapsed     time.Duration
	// Errors は表示用に保持する失敗の詳細です。
	// maxReportedErrors 件で打ち切られます。
	Errors []error
}

// writeSummary は結果の要約を書き出します。
func (r *copyResult) writeSummary(w io.Writer) {
	fmt.Fprintf(w, "\nコピー完了: %d件成功, %d件失敗", r.Transferred, r.Failed)
	if r.Elapsed > 0 {
		fmt.Fprintf(w, " (%s)", r.Elapsed.Round(time.Millisecond))
	}
	fmt.Fprintln(w)
	if len(r.Errors) == 0 {
		return
	}
	fmt.Fprintf(w, "\n失敗した内容:\n")
	for _, err := range r.Errors {
		fmt.Fprintf(w, "  %v\n", err)
	}
	if r.Failed > len(r.Errors) {
		fmt.Fprintf(w, "  ... ほか %d件\n", r.Failed-len(r.Errors))
	}
}

// copyTree は srcPath 以下を destDirPath へコピーします。
//
// 個々のファイルの失敗は集計され、すべて処理し終えてから copyResult として
// 返されます。以前はワーカー内のエラーを log で出すだけで握り潰しており、
// 呼び出し側からは常に成功したように見えていました。
//
// なお、この関数はビルトインの copy を隠さないよう copyTree という名前です。
func copyTree(srcStorage, destStorage hbg.Storage, srcPath, destDirPath string, updateDuration time.Duration, ignores []string, worker int) (*copyResult, error) {
	// worker が 0 だと容量0のチャネルとワーカー0個になり、
	// 最初の送信で永久にブロックする。
	if worker < 1 {
		worker = 1
	}

	// コピー元を先に解決しておく。
	// もとは一致するものがなくても「0件成功」として正常終了していたため、
	// コピー元のパスを打ち間違えてもスクリプトからは成功に見えていた。
	srcFileInfos, err := resolveSrcFileInfos(srcStorage, srcPath)
	if err != nil {
		return nil, err
	}

	copyFileArgs := []*copyFileArg{}

	// コピー対象を集める
	aggregateQ := make(chan *copyFileArg, worker)
	aggregateWG := &sync.WaitGroup{}
	aggregateWG.Add(1)
	go func() {
		defer aggregateWG.Done()
		for arg := range aggregateQ {
			copyFileArgs = append(copyFileArgs, arg)
		}
	}()
	err = aggregateCopyFileArgs(aggregateQ, srcStorage, destStorage, srcPath, destDirPath, updateDuration, ignores, srcFileInfos, nil)
	// エラーでも必ず閉じる。もとは早期 return しており、
	// 集約用のゴルーチンが残ったままになっていた。
	close(aggregateQ)
	aggregateWG.Wait()
	if err != nil {
		return nil, fmt.Errorf("error at cp: %w", err)
	}

	fmt.Printf("%dつのファイルのコピーを開始します\n", len(copyFileArgs))

	// コピーする
	started := time.Now()
	result := &copyResult{}
	mu := &sync.Mutex{}
	copyQ := make(chan *copyFileArg, worker)
	copyWG := &sync.WaitGroup{}
	for i := 0; i < worker; i++ {
		copyWG.Add(1)
		go func() {
			defer copyWG.Done()
			for arg := range copyQ {
				fileStarted := time.Now()
				bytes, err := copyFile(arg.srcStorage, arg.destStorage, arg.srcFilePath, arg.destDirPath)
				elapsed := time.Since(fileStarted)

				// 端末の表示とは独立して、転送1件につき1レコードを記録する。
				// あとから「どのファイルが失敗したか」を追えるようにするため。
				rec := hbglog.TransferRecord{
					SrcStorage: arg.srcStorage.Type(),
					SrcPath:    arg.srcFilePath,
					DstStorage: arg.destStorage.Type(),
					DstPath:    arg.destDirPath,
					Bytes:      bytes,
					Duration:   elapsed,
					Result:     hbglog.ResultCopied,
					Err:        err,
				}
				if err != nil {
					rec.Result = hbglog.ResultFailed
				}
				hbglog.LogTransfer(rec)

				mu.Lock()
				if err != nil {
					err = fmt.Errorf("%s:%s -> %s:%s: %w", arg.srcStorage.Type(), arg.srcFilePath, arg.destStorage.Type(), arg.destDirPath, err)
					result.Failed++
					if len(result.Errors) < maxReportedErrors {
						result.Errors = append(result.Errors, err)
					}
					log.Printf("%s", err)
				} else {
					result.Transferred++
				}
				mu.Unlock()
			}
		}()
	}
	for _, arg := range copyFileArgs {
		copyQ <- arg
	}
	close(copyQ)
	copyWG.Wait()

	result.Elapsed = time.Since(started)
	hbglog.LogSummary(result.Transferred, result.Failed, result.Elapsed)
	return result, nil
}

// ensureDestDir はコピー先ディレクトリを用意し、その中身を返します。
// ディレクトリが存在しない場合は作成します。
//
// もとは同じ処理が2箇所にコピーされていました。
func ensureDestDir(destStorage hbg.Storage, destDirPath string) ([]*hbg.FileInfo, error) {
	destFileInfos, err := destStorage.List(destDirPath)
	if err == nil {
		return destFileInfos, nil
	}

	// ディレクトリがないと List がエラーになりえるので、作ってから列挙し直す。
	if mkErr := mkDirWithRetry(destStorage, destDirPath); mkErr != nil {
		return nil, mkErr
	}

	destFileInfos, err = destStorage.List(destDirPath)
	if err != nil {
		return nil, fmt.Errorf("error at list directory %s:%s: %w", destStorage.Type(), destDirPath, err)
	}
	return destFileInfos, nil
}

// mkDirWithRetry はディレクトリの作成を試みます。
//
// クラウドストレージは短時間に叩きすぎると拒否されることがあるため、
// 間隔を空けて1度だけ再試行します。エラーの種類を見た本格的な再試行
// （指数バックオフ）は今後の課題です。
func mkDirWithRetry(destStorage hbg.Storage, destDirPath string) error {
	const retryInterval = time.Second

	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if destStorage.Type() != "local" {
			time.Sleep(retryInterval)
		}
		if err = destStorage.MkDir(destDirPath); err == nil {
			return nil
		}
	}
	return fmt.Errorf("error at create directory %s:%s: %w", destStorage.Type(), destDirPath, err)
}

// resolveSrcFileInfos は、コマンドラインで指定されたコピー元を解決します。
// 何にも一致しなかった場合はエラーを返します。
func resolveSrcFileInfos(srcStorage hbg.Storage, srcPath string) ([]*hbg.FileInfo, error) {
	parentDir := filepath.ToSlash(filepath.Dir(srcPath))
	srcFiles, err := srcStorage.List(parentDir)
	if err != nil {
		return nil, fmt.Errorf("failed list %s at %s: %w", parentDir, srcStorage.Type(), err)
	}

	srcFileInfos, err := glob(srcFiles, srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed glob %s. %w", srcPath, err)
	}
	if len(srcFileInfos) == 0 {
		return nil, fmt.Errorf("コピー元が見つかりません: %s:%s", srcStorage.Type(), srcPath)
	}
	return srcFileInfos, nil
}

// destFileInfosは、移動先フォルダをListしたもの。
// srcFileInfosは、移動元フォルダをListしたもの。
func aggregateCopyFileArgs(q chan *copyFileArg, srcStorage, destStorage hbg.Storage, srcPath, destDirPath string, updateDuration time.Duration, ignores []string, srcFileInfos []*hbg.FileInfo, destFileInfos []*hbg.FileInfo) error {
	// どちらもディレクトリの場合
	var err error

	if srcFileInfos == nil {
		parentDir := filepath.ToSlash(filepath.Dir(srcPath))
		srcFiles, listErr := srcStorage.List(parentDir)
		if listErr != nil {
			return fmt.Errorf("failed list %s at %s: %w", parentDir, srcStorage.Type(), listErr)
		}
		// もとは glob のエラーをブロックの外で検査していたが、
		// ブロック内で := により err がシャドーされていたため、
		// 外側の検査は常に nil を見る死にコードになっていた。
		srcFileInfos, err = glob(srcFiles, srcPath)
		if err != nil {
			return fmt.Errorf("failed glob %s. %w", srcPath, err)
		}
	}

	if destFileInfos == nil {
		destFileInfos, err = ensureDestDir(destStorage, destDirPath)
		if err != nil {
			return err
		}
	}

Loop:
	for _, srcFileInfo := range srcFileInfos {
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
				return fmt.Errorf("failed list %s at %s. %w", srcFileInfo.Name, srcStorage.Type(), err)
			}

			// もとはコピー先のパスを「子ファイルの親ディレクトリ」から導出していた。
			// そのため子を持たないディレクトリでは空文字になり、
			// コピー先に作られないばかりか、続く List("") が失敗していた。
			// コピー元のディレクトリ名から直接求める。
			childDestDirPath := filepath.ToSlash(filepath.Join(destDirPath, filepath.Base(srcFileInfo.Path)))

			// 中身が空でも、ここでコピー先のディレクトリが作られる。
			childDestFileInfos, err := ensureDestDir(destStorage, childDestDirPath)
			if err != nil {
				return err
			}

			for _, file := range files {
				if file.IsDir {
					err = aggregateCopyFileArgs(q, srcStorage, destStorage, filepath.ToSlash(file.Path), childDestDirPath, updateDuration, ignores, nil, nil)
				} else {
					// files は srcFileInfo.Path を列挙した結果そのものなので、
					// もとはここで同じディレクトリを List し直していた。
					// クラウドストレージでは無駄な往復になる。
					err = aggregateCopyFileArgs(q, srcStorage, destStorage, filepath.ToSlash(file.Path), childDestDirPath, updateDuration, ignores, []*hbg.FileInfo{file}, childDestFileInfos)
				}
				if err != nil {
					return err
				}
			}
			continue Loop
		}

		// ファイルで、
		// 最終更新時刻の差がそれ未満かつ、ファイルサイズが同一だったらスキップ
		if shouldSkipCopy(srcFileInfo, destFileInfos, updateDuration) {
			continue Loop
		}

		// コピー
		q <- &copyFileArg{
			srcStorage:  srcStorage,
			destStorage: destStorage,
			srcFilePath: srcFileInfo.Path,
			destDirPath: destDirPath,
		}
	}
	return nil
}

// shouldSkipCopy は、srcFileInfo をコピーせずスキップしてよいかを判定します。
//
// 判定規則は「コピー先に同名のファイルがあり、かつ
// 最終更新時刻の差が updateDuration 以内で、かつサイズが一致する」ことです。
//
// 注意: 時刻差は絶対値で比較しているため、コピー先のほうが新しい場合でも
// updateDuration を超えていればコピー対象になります（＝上書きされます）。
// これは既存の挙動であり、意図的にそのまま維持しています。
func shouldSkipCopy(srcFileInfo *hbg.FileInfo, destFileInfos []*hbg.FileInfo, updateDuration time.Duration) bool {
	for _, destFileInfo := range destFileInfos {
		if srcFileInfo.Name != destFileInfo.Name {
			continue
		}
		srcTimeUTC := srcFileInfo.LastMod.UTC()
		destTimeUTC := destFileInfo.LastMod.UTC()
		duration := srcTimeUTC.Sub(destTimeUTC)

		d := int64(duration)
		if d < 0 {
			d *= int64(-1)
		}
		if d <= int64(updateDuration) && srcFileInfo.Size == destFileInfo.Size {
			return true
		}
	}
	return false
}

type copyFileArg struct {
	srcStorage  hbg.Storage
	destStorage hbg.Storage
	srcFilePath string
	destDirPath string
}

// copyFile は1ファイルをコピーし、転送したバイト数を返します。
func copyFile(srcStorage, destStorage hbg.Storage, srcFilePath, destDirPath string) (int64, error) {
	fmt.Printf("copy %s:%s -> %s:%s\n", srcStorage.Type(), srcFilePath, destStorage.Type(), destDirPath)

	file, err := srcStorage.Get(srcFilePath)
	if err != nil {
		return 0, fmt.Errorf("error at get %s:%s : %w", srcStorage.Type(), srcFilePath, err)
	}
	defer file.Data.Close()

	if err := destStorage.Push(destDirPath, file); err != nil {
		return 0, fmt.Errorf("error at push from %s:%s to %s:%s : %w", srcStorage.Type(), srcFilePath, destStorage.Type(), destDirPath, err)
	}
	return file.Size, nil
}
