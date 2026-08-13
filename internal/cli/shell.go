package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/mt3hr/hbg/internal/hbghome"
	hbgstorage "github.com/mt3hr/hbg/storage"
	"github.com/spf13/cobra"
)

var (
	shellCmd = &cobra.Command{
		Use:   "shell",
		Short: "対話シェルを起動する",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			resolver, err := resolverFromConfig(config)
			if err != nil {
				return withExitCode(ExitUsage, err)
			}
			defer resolver.Close()

			// ストレージは使われるまで組み立てない。
			// シェルを開くだけでは、設定にあるクラウドの認証は走らない。
			storages := newLazyStorages(ctx, resolver)

			// OS のパス規則に従うストレージ（ローカル）を起点にする。
			var currentStorage hbgstorage.Storage
			for _, name := range resolver.Names() {
				s, err := storages.get(name)
				if err != nil {
					continue
				}
				if s.Features().OSPath {
					currentStorage = s
					abs, err := filepath.Abs(".")
					if err != nil {
						return fmt.Errorf("failed get working directory. %w", err)
					}
					storages.setPath(s, filepath.ToSlash(abs))
					break
				}
			}
			if currentStorage == nil {
				return withExitCode(ExitUsage,
					fmt.Errorf("ローカルのストレージが設定されていないため shell を開始できません"))
			}
			currentPathMap := storages.paths

			// 履歴は一時ディレクトリではなく $HOME/hbg 配下に置く。
			historyFile, err := hbghome.ShellHistoryFile()
			if err != nil {
				return err
			}
			if err := hbghome.EnsureParentDir(historyFile); err != nil {
				return err
			}

			// readline のインスタンスはループの外で1度だけ作る。
			// もとはループのたびに生成して defer で閉じており、
			// 履歴ファイルのハンドルがプロセスの生存期間ぶん積み上がっていた。
			l, err := readline.NewEx(&readline.Config{
				HistoryFile:     historyFile,
				InterruptPrompt: "^C",
				EOFPrompt:       "exit",
			})
			if err != nil {
				return fmt.Errorf("failed to initialize shell. %w", err)
			}
			defer l.Close()

		Loop:
			for {
				currentPath := currentPathMap[currentStorage]
				prompt := fmt.Sprintf("%s:%s > ", currentStorage.Name(), currentPath)

				pathResolute := func(storage hbgstorage.Storage, p string, dirOnly bool) (string, error) {
					p = path.Clean(p)
					if storage.Type() == "local" {
						p = os.ExpandEnv(p)
						p = filepath.ToSlash(p)
					}

					if strings.HasPrefix(p, "/") ||
						strings.HasPrefix(p, "A:") ||
						strings.HasPrefix(p, "B:") ||
						strings.HasPrefix(p, "C:") ||
						strings.HasPrefix(p, "D:") ||
						strings.HasPrefix(p, "E:") ||
						strings.HasPrefix(p, "F:") ||
						strings.HasPrefix(p, "G:") ||
						strings.HasPrefix(p, "H:") ||
						strings.HasPrefix(p, "I:") ||
						strings.HasPrefix(p, "J:") ||
						strings.HasPrefix(p, "K:") ||
						strings.HasPrefix(p, "L:") ||
						strings.HasPrefix(p, "M:") ||
						strings.HasPrefix(p, "N:") ||
						strings.HasPrefix(p, "O:") ||
						strings.HasPrefix(p, "P:") ||
						strings.HasPrefix(p, "Q:") ||
						strings.HasPrefix(p, "R:") ||
						strings.HasPrefix(p, "S:") ||
						strings.HasPrefix(p, "T:") ||
						strings.HasPrefix(p, "U:") ||
						strings.HasPrefix(p, "V:") ||
						strings.HasPrefix(p, "W:") ||
						strings.HasPrefix(p, "X:") ||
						strings.HasPrefix(p, "Y:") ||
						strings.HasPrefix(p, "Z:") {
					} else {
						p = path.Join(currentPathMap[storage], p)
						stat, err := storage.Stat(ctx, p)
						if stat == nil {
							_ = err
							return p, nil
							// cpのときにpatternで指定し得るので
							// err := fmt.Errorf("そんなファイルはないかもしれません。%w", err)
							// return "", err
						}
						if !((stat.IsDir && dirOnly) || !dirOnly) {
							err := fmt.Errorf("%sはファイルです。", p)
							return "", err

						}
						return p, nil
					}
					return p, nil
				}

				trimPrefix := func(str string) string {
					str = strings.TrimSpace(strings.TrimPrefix(str, "ls"))
					str = strings.TrimSpace(strings.TrimPrefix(str, "cp"))
					str = strings.TrimSpace(strings.TrimPrefix(str, "rm"))
					str = strings.TrimSpace(strings.TrimPrefix(str, "cd"))
					return str
				}

				listFilesFunc := func(storage hbgstorage.Storage, dirOnly bool) func(string) []string {
					return func(file string) []string {
						file = trimPrefix(file)
						arg := file
						childItems := []string{}
						currentPath := currentPathMap[storage]

						if strings.HasPrefix(file, "/") ||
							strings.HasPrefix(file, "A:") ||
							strings.HasPrefix(file, "B:") ||
							strings.HasPrefix(file, "C:") ||
							strings.HasPrefix(file, "D:") ||
							strings.HasPrefix(file, "E:") ||
							strings.HasPrefix(file, "F:") ||
							strings.HasPrefix(file, "G:") ||
							strings.HasPrefix(file, "H:") ||
							strings.HasPrefix(file, "I:") ||
							strings.HasPrefix(file, "J:") ||
							strings.HasPrefix(file, "K:") ||
							strings.HasPrefix(file, "L:") ||
							strings.HasPrefix(file, "M:") ||
							strings.HasPrefix(file, "N:") ||
							strings.HasPrefix(file, "O:") ||
							strings.HasPrefix(file, "P:") ||
							strings.HasPrefix(file, "Q:") ||
							strings.HasPrefix(file, "R:") ||
							strings.HasPrefix(file, "S:") ||
							strings.HasPrefix(file, "T:") ||
							strings.HasPrefix(file, "U:") ||
							strings.HasPrefix(file, "V:") ||
							strings.HasPrefix(file, "W:") ||
							strings.HasPrefix(file, "X:") ||
							strings.HasPrefix(file, "Y:") ||
							strings.HasPrefix(file, "Z:") {

							existFile := false
							var stat *hbgstorage.FileInfo
							if file != "" {
								stat, _ = storage.Stat(ctx, file)
								if stat != nil {
									existFile = true
								}
							}
							if !existFile {
								file = strings.TrimPrefix(file, currentPath)

								isRootItem := strings.HasPrefix(file, "/")
								if !isRootItem {
									file = strings.TrimPrefix(file, "/")
								} else {
									file = "/" + file
								}

								stat, err = storage.Stat(ctx, file)
								if err == nil {
									existFile = true
								} else {
									file = filepath.ToSlash(filepath.Dir(file))

									if file == "." {
										file = "/"
									}

									stat, err = storage.Stat(ctx, file)
									if err == nil {
										if stat.IsDir || !dirOnly {
											existFile = true
										}
									}
								}
							}

							if existFile {
								if stat.IsDir || !dirOnly {
									files, err := hbgstorage.ListAll(ctx, storage, file)
									if err != nil {
										// 補完候補の取得に失敗しても、シェルの操作は継続できる。
										// 候補なしとして扱う。
										files = nil
									}
									for _, f := range files {
										if f.IsDir || !dirOnly {
											dirName := path.Join(file, f.Name)
											dirName = filepath.ToSlash(dirName)
											childItems = append(childItems, dirName)
										}
									}
								}
							}
							sort.Slice(childItems, func(i, j int) bool { return childItems[i] < childItems[j] })

							return childItems
						}
						file = arg
						currentChildItems := []string{}
						if file == "" {
							files, err := hbgstorage.ListAll(ctx, storage, currentPath)
							if err != nil {
								// 補完候補の取得に失敗しても、シェルの操作は継続できる。
								// 候補なしとして扱う。
								files = nil
							}
							for _, f := range files {
								if f.IsDir || !dirOnly {
									filepath := filepath.ToSlash(path.Clean(f.Name))
									currentChildItems = append(currentChildItems, filepath)
								}
							}
						} else {
							existFile := false
							var stat *hbgstorage.FileInfo
							if file != "" {
								stat, _ = storage.Stat(ctx, file)
								if stat != nil {
									existFile = true
								}
							}
							if !existFile {
								file = strings.TrimPrefix(file, currentPath)
								file = strings.TrimPrefix(file, "/")
								file = path.Join(currentPath, file)

								stat, err = storage.Stat(ctx, file)
								if err == nil {
									existFile = true
								} else {
									file = filepath.ToSlash(filepath.Dir(file))

									stat, err = storage.Stat(ctx, file)
									if err == nil {
										if stat.IsDir || !dirOnly {
											existFile = true
										}
									}
								}
							}

							files, err := hbgstorage.ListAll(ctx, storage, file)
							if err != nil {
								// log.Fatal(err)
							}
							for _, f := range files {
								if f.IsDir || !dirOnly {
									file := strings.TrimPrefix(file, currentPath)
									file = filepath.ToSlash(path.Clean(path.Join(file, f.Name)))
									file = strings.TrimPrefix(file, "/")
									currentChildItems = append(currentChildItems, file)
								}
							}
						}
						sort.Slice(currentChildItems, func(i, j int) bool { return currentChildItems[i] < currentChildItems[j] })
						return currentChildItems
					}
				}

				listStorageFilesFunc := func(file string) []string {
					file = trimPrefix(file)

					spl := strings.SplitN(file, " ", 2)
					if len(spl) == 2 {
						file = strings.TrimSpace(spl[1])
					}

					for _, name := range storages.names() {
						file = strings.TrimSpace(strings.TrimPrefix(file, name+":"))
					}

					childItems := []string{}
					// 補完候補は、すでに組み立て済みのストレージからだけ集める。
					// 補完のために未使用のクラウドへ接続しにいくのは避ける。
					for _, storage := range storages.opened() {
						for _, file := range listFilesFunc(storage, false)(file) {
							filename := storage.Name() + ":" + file
							if len(spl) == 2 {
								filename = spl[0] + " " + filename
							}
							childItems = append(childItems, filename)
						}
					}
					sort.Slice(childItems, func(i, j int) bool { return childItems[i] < childItems[j] })
					return childItems
				}

				listStorages := func(_ string) []string {
					// 名前の一覧は組み立てずに取れる
					return storages.names()
				}

				completer := readline.NewPrefixCompleter(
					readline.PcItem("cd", readline.PcItemDynamic(listFilesFunc(currentStorage, true))),
					readline.PcItem("cs", readline.PcItemDynamic(listStorages)),
					readline.PcItem("pwd"),
					readline.PcItem("ls", readline.PcItemDynamic(listFilesFunc(currentStorage, true))),
					readline.PcItem("cp", readline.PcItemDynamic(listStorageFilesFunc)),

					readline.PcItem("rm", readline.PcItemDynamic(listFilesFunc(currentStorage, false))),
					readline.PcItem("exit"),
				)

				// 補完候補は現在のストレージに依存するので毎回入れ替える。
				l.Config.AutoComplete = completer
				l.SetPrompt(prompt)

				line, err := l.Readline()
				if err != nil {
					// Ctrl-D（EOF）や Ctrl-C は異常ではなく終了操作。
					// もとは log.Fatal でプロセスを落としていた。
					if errors.Is(err, io.EOF) || errors.Is(err, readline.ErrInterrupt) {
						return nil
					}
					return fmt.Errorf("failed read line. %w", err)
				}
				line = strings.TrimSpace(line)

				// コマンド
				if line == "exit" {
					return nil
				}
				if strings.HasPrefix(line, "ls") {
					spl := strings.SplitN(line, " ", 2)
					if len(spl) == 1 {
						err := list(ctx, currentStorage, currentPath, true, true)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
					} else {
						dir := spl[1]
						dir, err := pathResolute(currentStorage, dir, true)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}

						err = list(ctx, currentStorage, dir, true, true)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
					}
				}
				if strings.HasPrefix(line, "cd") {
					spl := strings.SplitN(line, " ", 2)
					if len(spl) != 1 {
						dir := spl[1]

						currentPath, err = pathResolute(currentStorage, dir, true)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}

						currentPath = filepath.ToSlash(currentPath)
						currentPathMap[currentStorage] = currentPath
					}
				}
				if line == "pwd" {
					fmt.Println(currentStorage.Name() + ":" + currentPath)
				}
				if strings.HasPrefix(line, "cs") {
					spl := strings.SplitN(line, " ", 2)
					if len(spl) != 1 {
						storageName := spl[1]
						// ここで初めて対象のストレージが組み立てられる
						s, err := storages.get(storageName)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
						currentStorage = s
						continue Loop
					}
				}
				if strings.HasPrefix(line, "rm") {
					spl := strings.SplitN(line, " ", 2)
					if len(spl) != 1 {
						target := spl[1]
						err := remove(ctx, currentStorage, target)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
					}
				}
				if strings.HasPrefix(line, "cp") {
					spl := strings.SplitN(line, " ", 3)
					if len(spl) == 3 {
						src := strings.TrimSpace(spl[1])
						dest := strings.TrimSpace(spl[2])

						srcSpl := strings.SplitN(src, ":", 2)
						destSpl := strings.SplitN(dest, ":", 2)

						if len(srcSpl) != 2 {
							fmt.Println("srcを正しく指定してください")
							continue Loop
						}
						if len(destSpl) != 2 {
							fmt.Println("destを正しく指定してください")
							continue Loop
						}

						srcStorage, err := storages.get(srcSpl[0])
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
						destStorage, err := storages.get(destSpl[0])
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
						srcPath := srcSpl[1]
						destPath := destSpl[1]
						// もとは空のスライスに //TODO と書かれており、
						// シェルの cp だけ無視リストが効いていなかった。
						ignores := defaultIgnores

						srcPath, err = pathResolute(srcStorage, srcPath, false)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}

						destPath, err = pathResolute(destStorage, destPath, false)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}

						// もとは戻り値を捨てており、コピーに失敗しても
						// シェル上には何も表示されなかった。
						result, err := copyTree(ctx, srcStorage, destStorage, srcPath, destPath, time.Second, ignores, 1)
						if err != nil {
							fmt.Println(err.Error())
							continue Loop
						}
						result.writeSummary(os.Stdout)
					}
				}
			}
		},
	}
)
