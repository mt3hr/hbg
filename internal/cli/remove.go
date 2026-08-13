package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/mt3hr/hbg/storage"
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

func runRemove(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	s, err := resolver.Get(ctx, removeOpt.targetStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	return remove(ctx, s, removeOpt.targetPath)
}

// remove は指定されたパスを中身ごと削除します。
func remove(ctx context.Context, s storage.Storage, path string) error {
	// もとはエラーメッセージに引数の path ではなく
	// パッケージ変数を使っていたため、シェルなど別経路から
	// 呼んだときに誤ったパスが表示されていた。
	if err := storage.PurgeAll(ctx, s, path); err != nil {
		return fmt.Errorf("error at delete %s. %w", path, err)
	}
	return nil
}
