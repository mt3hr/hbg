package cmd

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"bitbucket.org/mt3hr/hbg"
	"github.com/chzyer/readline"
	"github.com/spf13/cobra"
)

var (
	shellCmd = &cobra.Command{
		Use: "shell",
		Run: func(_ *cobra.Command, _ []string) {
			storages, err := storageMapFromConfig(cfg)
			if err != nil {
				err = fmt.Errorf("failed load storages. %w", err)
				log.Fatal(err)
			}

			var currentStorage hbg.Storage
			currentPathMap := map[hbg.Storage]string{}
			if err != nil {
				err = fmt.Errorf("failed get working directory. %w", err)
				log.Fatal(err)
			}
			for _, storage := range storages {
				if storage.Type() == "local" {
					currentStorage = storage
					currentPathMap[storage], err = filepath.Abs(".")
					currentPathMap[storage] = filepath.ToSlash(currentPathMap[storage])
					if err != nil {
						log.Fatal(err)
					}
				} else {
					currentPathMap[storage] = "/"
				}
			}

		Loop:
			for {
				currentPath := currentPathMap[currentStorage]
				prompt := fmt.Sprintf("%s:%s > ", currentStorage.Name(), currentPath)

				listStorageFilesFunc := func(file string) []string {
					file = strings.TrimSpace(strings.TrimPrefix(file, "cp"))
					file = strings.TrimSpace(strings.TrimPrefix(file, "rm"))
					file = strings.TrimSpace(strings.TrimPrefix(file, "cd"))
					for _, storage := range storages {
						file = strings.TrimSpace(strings.TrimPrefix(file, storage.Name()+":"))
					}

					childItems := []string{}
					for _, storage := range storages {
						file := file
						currentPath := currentPathMap[storage]

						existFile := false
						var stat *hbg.FileInfo
						if file != "" {
							stat, _ = storage.Stat(file)
							if stat != nil {
								existFile = true
							}
						}
						if !existFile {
							file = strings.TrimPrefix(file, currentPath)
							file = path.Join(currentPath, file)

							stat, err = storage.Stat(file)
							if err == nil {
								existFile = true
							} else {
								file = filepath.ToSlash(filepath.Dir(file))
								stat, err = storage.Stat(file)
								if err == nil {
									existFile = true
								}
							}
						}

						if existFile {
							files, err := storage.List(file)
							if err != nil {
								log.Fatal(err)
							}
							for _, f := range files {
								dirName := path.Join(file, f.Name)
								storagePath := storage.Name() + ":" + dirName
								childItems = append(childItems, storagePath)
							}
						}
					}
					sort.Slice(childItems, func(i, j int) bool { return childItems[i] < childItems[j] })
					return childItems
				}

				listFileAndDirsFunc := func(storage hbg.Storage) func(string) []string {
					return func(file string) []string {
						file = strings.TrimSpace(strings.TrimPrefix(file, "cp"))
						file = strings.TrimSpace(strings.TrimPrefix(file, "rm"))
						file = strings.TrimSpace(strings.TrimPrefix(file, "cd"))

						childItems := []string{}
						currentPath := currentPathMap[storage]

						existFile := false
						var stat *hbg.FileInfo
						if file != "" {
							stat, _ = storage.Stat(file)
							if stat != nil {
								existFile = true
							}
						}
						if !existFile {
							file = strings.TrimPrefix(file, currentPath)
							file = path.Join(currentPath, file)

							stat, err = storage.Stat(file)
							if err == nil {
								existFile = true
							} else {
								file = filepath.ToSlash(filepath.Dir(file))
								stat, err = storage.Stat(file)
								if err == nil {
									existFile = true
								}
							}
						}

						if existFile {
							files, err := storage.List(file)
							if err != nil {
								log.Fatal(err)
							}
							for _, f := range files {
								dirName := path.Join(file, f.Name)
								dirName = filepath.ToSlash(dirName)
								childItems = append(childItems, dirName)
							}
						}
						sort.Slice(childItems, func(i, j int) bool { return childItems[i] < childItems[j] })
						return childItems
					}
				}

				listDirsFunc := func(storage hbg.Storage) func(string) []string {
					return func(dir string) []string {
						dir = strings.TrimSpace(strings.TrimPrefix(dir, "cp"))
						dir = strings.TrimSpace(strings.TrimPrefix(dir, "rm"))
						dir = strings.TrimSpace(strings.TrimPrefix(dir, "cd"))

						childItems := []string{}
						currentPath := currentPathMap[storage]

						existDir := false
						var stat *hbg.FileInfo
						if dir != "" {
							stat, _ = storage.Stat(dir)
							if stat != nil {
								existDir = true
							}
						}
						if !existDir {
							dir = strings.TrimPrefix(dir, currentPath)
							dir = path.Join(currentPath, dir)

							stat, err = storage.Stat(dir)
							if err == nil {
								existDir = true
							} else {
								dir = filepath.ToSlash(filepath.Dir(dir))
								stat, err = storage.Stat(dir)
								if err == nil {
									if stat.IsDir {
										existDir = true
									}
								}
							}
						}

						if existDir {
							if stat.IsDir {
								files, err := storage.List(dir)
								if err != nil {
									log.Fatal(err)
								}
								for _, f := range files {
									if f.IsDir {
										dirName := path.Join(dir, f.Name)
										dirName = filepath.ToSlash(dirName)
										childItems = append(childItems, dirName)
									}
								}
							}
						}
						sort.Slice(childItems, func(i, j int) bool { return childItems[i] < childItems[j] })
						return childItems
					}
				}

				listStorages := func(_ string) []string {
					storageNames := []string{}
					for _, storage := range storages {
						storageNames = append(storageNames, storage.Name())
					}
					sort.Slice(storageNames, func(i, j int) bool { return storageNames[i] < storageNames[j] })
					return storageNames
				}

				completer := readline.NewPrefixCompleter(
					readline.PcItem("cd", readline.PcItemDynamic(listDirsFunc(currentStorage))),
					readline.PcItem("cs", readline.PcItemDynamic(listStorages)),
					readline.PcItem("pwd"),
					readline.PcItem("ls"),
					readline.PcItem("cp", readline.PcItemDynamic(listStorageFilesFunc)),
					readline.PcItem("rm", readline.PcItemDynamic(listFileAndDirsFunc(currentStorage))),
					readline.PcItem("exit"),
				)

				historyFile := filepath.Join(os.TempDir(), "hbg_history")

				l, err := readline.NewEx(&readline.Config{
					HistoryFile:     historyFile,
					Prompt:          prompt,
					AutoComplete:    completer,
					InterruptPrompt: "^C",
					EOFPrompt:       "exit",
				})
				if err != nil {
					log.Fatal(err)
				}
				defer l.Close()

				line, err := l.Readline()
				if err != nil {
					err = fmt.Errorf("failed read line. %w", err)
					log.Fatal(err)
				}
				line = strings.TrimSpace(line)

				// コマンド
				if line == "exit" {
					return
				}
				if line == "ls" {
					err := list(currentStorage, currentPath, true, true)
					if err != nil {
						log.Fatal(err)
					}
				}
				if strings.HasPrefix(line, "cd") {
					spl := strings.SplitN(line, " ", 2)
					if len(spl) != 1 {
						dir := spl[1]

						if currentStorage.Type() == "local" {
							dir = os.ExpandEnv(dir)
							dir = filepath.ToSlash(dir)
						}

						if strings.Contains(dir, "..") {
							dir = path.Clean(dir)
						}
						if strings.HasPrefix(dir, "/") ||
							strings.HasPrefix(dir, "A:") ||
							strings.HasPrefix(dir, "B:") ||
							strings.HasPrefix(dir, "C:") ||
							strings.HasPrefix(dir, "D:") ||
							strings.HasPrefix(dir, "E:") ||
							strings.HasPrefix(dir, "F:") ||
							strings.HasPrefix(dir, "G:") ||
							strings.HasPrefix(dir, "H:") ||
							strings.HasPrefix(dir, "I:") ||
							strings.HasPrefix(dir, "J:") ||
							strings.HasPrefix(dir, "K:") ||
							strings.HasPrefix(dir, "L:") ||
							strings.HasPrefix(dir, "M:") ||
							strings.HasPrefix(dir, "N:") ||
							strings.HasPrefix(dir, "O:") ||
							strings.HasPrefix(dir, "P:") ||
							strings.HasPrefix(dir, "Q:") ||
							strings.HasPrefix(dir, "R:") ||
							strings.HasPrefix(dir, "S:") ||
							strings.HasPrefix(dir, "T:") ||
							strings.HasPrefix(dir, "U:") ||
							strings.HasPrefix(dir, "V:") ||
							strings.HasPrefix(dir, "W:") ||
							strings.HasPrefix(dir, "X:") ||
							strings.HasPrefix(dir, "Y:") ||
							strings.HasPrefix(dir, "Z:") {
							currentPath = dir
						} else {
							currentPath = path.Join(currentPathMap[currentStorage], dir)
							stat, _ := currentStorage.Stat(currentPath)
							if stat == nil {
								fmt.Println("そんなディレクトリはないかもしれません。")
								continue Loop
							}
							if !stat.IsDir {
								fmt.Printf("%sはファイルです。", dir)
								continue Loop
							}
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
						for _, storage := range storages {
							if storage.Name() == storageName {
								currentStorage = storage
								continue Loop
							}
						}
					}
				}
				if strings.HasPrefix(line, "rm") {
					spl := strings.SplitN(line, " ", 2)
					if len(spl) != 1 {
						target := spl[1]
						err := currentStorage.Delete(target)
						if err != nil {
							log.Fatal(err)
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

						srcStorage := storages[srcSpl[0]]
						destStorage := storages[destSpl[0]]
						srcPath := srcSpl[1]
						destPath := destSpl[1]
						ignores := []string{} //TODO

						expandPathFunc := func(storage hbg.Storage, file string) string {
							if storage.Type() == "local" {
								file = os.ExpandEnv(file)
								file = filepath.ToSlash(file)
							}

							if strings.Contains(file, "..") {
								file = path.Clean(file)
							}
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
								return file
							} else {
								file = path.Join(currentPathMap[storage], file)
								return file
							}
						}
						srcPath = expandPathFunc(srcStorage, srcPath)
						destPath = expandPathFunc(destStorage, destPath)

						copy(srcStorage, destStorage, srcPath, destPath, time.Second, ignores, 1)
					}
				}
			}
		},
	}
)
