package cmd

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mt3hr/hbg"
	"github.com/spf13/cobra"
)

var (
	listCmd = &cobra.Command{
		Aliases: []string{"ls"},
		Run:     runList,
		Args:    cobra.ExactArgs(1),
		Use:     "list storage:path",
		Short:   "ストレージのファイルを一覧表示する",
		PreRun: func(_ *cobra.Command, args []string) {
			targetInfo := args[0]
			targetSplit := strings.SplitN(targetInfo, ":", 2)

			if len(targetSplit) < 2 {
				err := fmt.Errorf("pathの記述が変です")
				log.Fatal(err)
			}
			listOpt.targetStorage = targetSplit[0]
			listOpt.targetPath = targetSplit[1]
		},
	}
	listOpt = &struct {
		targetStorage string
		targetPath    string
		long          bool
		humanReadable bool
	}{}
)

func init() {
	fs := listCmd.Flags()
	fs.BoolVarP(&listOpt.long, "long", "l", false, "")
	fs.BoolVarP(&listOpt.humanReadable, "human-readable", "r", false, "")
}

const (
	_ = iota
	// KB .
	KB int64 = 1 << (10 * iota)
	// MB .
	MB
	// GB .
	GB
	// TB .
	TB
)

func runList(_ *cobra.Command, _ []string) {
	storages, err := storageMapFromConfig(cfg)
	if err != nil {
		err = fmt.Errorf("load storage failed. %w", err)
		log.Fatal(err)
	}
	storage, exist := storages[listOpt.targetStorage]
	if !exist {
		err = fmt.Errorf("not found storage '%s'. %w", listOpt.targetStorage, err)
		log.Fatal(err)
	}
	err = list(storage, listOpt.targetPath, listOpt.long, listOpt.humanReadable)
	if err != nil {
		log.Fatal(err)
	}
}

func list(storage hbg.Storage, path string, long, humanReadable bool) error {
	fileAndDirs, err := storage.List(path)
	if err != nil {
		err = fmt.Errorf("error at list at %s. %w", path, err)
		return err
	}

	files, dirs := []*hbg.FileInfo{}, []*hbg.FileInfo{}
	for _, f := range fileAndDirs {
		if f.IsDir {
			dirs = append(dirs, f)
		} else {
			files = append(files, f)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name < dirs[j].Name })
	fileAndDirs = append(dirs, files...)

	w := &tabwriter.Writer{}
	w.Init(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	for _, file := range fileAndDirs {
		isDir := ""
		timestr := ""
		sizestr := ""
		if file.IsDir {
			isDir = "dir"
		} else {
			isDir = "file"
			timestr = file.LastMod.Format(time.RFC3339)

			if humanReadable {
				sizestr = func() string {
					sizestr := ""
					if file.Size != 0 {
						tb := file.Size / TB
						gb := (file.Size % TB) / GB
						mb := ((file.Size % TB) % GB) / MB
						kb := (((file.Size % TB) % GB) % MB) / KB
						b := file.Size
						if kb != 0 {
							b = (((file.Size % TB) % GB) % MB) % kb
						}

						if tb != 0 {
							sizestr += strconv.FormatInt(tb, 10) + "."
							sizestr += strconv.FormatInt(gb/100, 10) + "T"
							return sizestr
						}
						if gb != 0 {
							sizestr += strconv.FormatInt(gb, 10) + "."
							sizestr += strconv.FormatInt(mb/100, 10) + "G"
							return sizestr
						}
						if mb != 0 {
							sizestr += strconv.FormatInt(mb, 10) + "."
							sizestr += strconv.FormatInt(kb/100, 10) + "M"
							return sizestr
						}
						if kb != 0 {
							sizestr += strconv.FormatInt(kb, 10) + "."
							sizestr += strconv.FormatInt(b/100, 10) + "K"
							return sizestr
						}
						if b != 0 {
							sizestr += strconv.FormatInt(b, 10) + "B"
							return sizestr
						}
					}
					return "0B"
				}()
			} else {
				sizestr = strconv.FormatInt(file.Size, 10)
				if err != nil {
					err = fmt.Errorf("failed parse int %d. %w", file.Size, err)
					log.Fatal(err)
				}
			}
		}

		fmt.Fprintf(w, "%s", file.Name)
		if long {
			fmt.Fprintf(w, "\t%s\t%s\t%s",
				isDir,
				timestr,
				sizestr,
			)
		}
		fmt.Fprintf(w, "\n")
	}
	err = w.Flush()
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
