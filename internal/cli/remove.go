package cli

import (
	"fmt"
	"strings"

	"github.com/mt3hr/hbg"
	"github.com/spf13/cobra"
)

var (
	removeCmd = &cobra.Command{
		Aliases: []string{"rm", "delete"},
		RunE:    runRemove,
		Args:    cobra.ExactArgs(1),
		Use:     "remove storage:path",
		Short:   "ストレージからファイルやディレクトリを削除する",
		PreRunE: func(_ *cobra.Command, args []string) error {
			targetInfo := args[0]
			targetSplit := strings.SplitN(targetInfo, ":", 2)

			if len(targetSplit) < 2 {
				return withExitCode(ExitUsage, fmt.Errorf("pathの記述が変です: %q（storage:path の形式で指定してください）", targetInfo))
			}
			removeOpt.targetStorage = targetSplit[0]
			removeOpt.targetPath = targetSplit[1]
			return nil
		},
	}
	removeOpt = &struct {
		targetStorage string
		targetPath    string
	}{}
)

func runRemove(_ *cobra.Command, _ []string) error {
	storages, err := storageMapFromConfig(config)
	if err != nil {
		return fmt.Errorf("load storage failed. %w", err)
	}
	storage, exist := storages[removeOpt.targetStorage]
	if !exist {
		return withExitCode(ExitUsage, fmt.Errorf("not found storage '%s'", removeOpt.targetStorage))
	}
	return remove(storage, removeOpt.targetPath)
}

func remove(storage hbg.Storage, path string) error {
	// もとはエラーメッセージに引数の path ではなく
	// removeOpt.targetPath を使っていたため、シェルなど
	// 別経路から呼んだときに誤ったパスが表示されていた。
	if err := storage.Delete(path); err != nil {
		return fmt.Errorf("error at delete %s. %w", path, err)
	}
	return nil
}
