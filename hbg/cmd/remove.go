package cmd

import (
	"fmt"
	"log"
	"strings"

	"github.com/mt3hr/hbg"
	"github.com/spf13/cobra"
)

var (
	removeCmd = &cobra.Command{
		Aliases: []string{"rm", "delete"},
		Run:     runRemove,
		Args:    cobra.ExactArgs(1),
		Use:     "remove storage:path",
		Short:   "ストレージからファイルやディレクトリを削除する",
		PreRun: func(_ *cobra.Command, args []string) {
			targetInfo := args[0]
			targetSplit := strings.SplitN(targetInfo, ":", 2)

			if len(targetSplit) < 2 {
				err := fmt.Errorf("pathの記述が変です")
				log.Fatal(err)
			}
			removeOpt.targetStorage = targetSplit[0]
			removeOpt.targetPath = targetSplit[1]
		},
	}
	removeOpt = &struct {
		targetStorage string
		targetPath    string
	}{}
)

func runRemove(_ *cobra.Command, _ []string) {
	storages, err := storageMapFromConfig(cfg)
	if err != nil {
		err = fmt.Errorf("load storage failed. %w", err)
		log.Fatal(err)
	}
	storage, exist := storages[removeOpt.targetStorage]
	if !exist {
		err = fmt.Errorf("not found storage '%s'. %w", removeOpt.targetStorage, err)
		log.Fatal(err)
	}
	err = remove(storage, removeOpt.targetPath)
	if err != nil {
		log.Fatal(err)
	}
}

func remove(storage hbg.Storage, path string) error {
	err := storage.Delete(path)
	if err != nil {
		err = fmt.Errorf("error at delete %s. %w", removeOpt.targetPath, err)
		return err
	}
	return nil
}
