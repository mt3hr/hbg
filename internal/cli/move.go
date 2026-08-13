package cli

import (
	"fmt"
	"strings"

	"github.com/mt3hr/hbg/storage"
	"github.com/spf13/cobra"
)

// 1つのストレージの中での移動と、ディレクトリの作成です。
//
// 移動は、同じストレージの中なら中身を運ばずに済みます。
// 別のストレージへ移したい場合は copy してから remove してください。
// 1つの操作にまとめると、コピーに失敗したのに元を消してしまう、
// といった事故が起こりうるためです。

var moveCmd = &cobra.Command{
	Use:     "move storage:srcPath storage:destPath",
	Aliases: []string{"mv"},
	Short:   "同じストレージの中でファイルを移動・改名する",
	Long: `同じストレージの中でファイルやディレクトリを移動・改名します。

中身は運ばれないので、大きなファイルでもすぐ終わります。

別のストレージへ移すことはできません。copy してから remove して
ください。1つの操作にまとめると、コピーに失敗したのに元を消して
しまう、といった事故が起こりえます。`,
	Example: `使用例
hbg move local:C:/photos/a.jpg local:C:/photos/2024/a.jpg
hbg move dropbox:/古い名前 dropbox:/新しい名前
`,
	Args: cobra.ExactArgs(2),
	RunE: runMove,
}

var mkdirCmd = &cobra.Command{
	Use:   "mkdir storage:path",
	Short: "ディレクトリを作る",
	Long: `ディレクトリを作ります。途中のディレクトリも必要なら作ります。
すでにある場合は何もしません。`,
	Example: `使用例
hbg mkdir dropbox:/backup/2024
`,
	Args: cobra.ExactArgs(1),
	RunE: runMkdir,
}

func runMove(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	srcName, srcPath, err := splitStoragePath(args[0])
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	dstName, dstPath, err := splitStoragePath(args[1])
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	if srcName != dstName {
		return withExitCode(ExitUsage, fmt.Errorf(
			"move は同じストレージの中だけです（%s と %s が指定されました）。\n"+
				"  別のストレージへ移す場合は copy してから remove してください",
			srcName, dstName))
	}

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	s, err := resolver.Get(ctx, srcName)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	if err := storage.Move(ctx, s, srcPath, dstPath); err != nil {
		if isCanceled(err) {
			return withExitCode(ExitInterrupted, fmt.Errorf("中断しました"))
		}
		return err
	}

	fmt.Printf("%s:%s を %s へ移動しました。\n", srcName, srcPath, dstPath)
	return nil
}

func runMkdir(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	name, dir, err := splitStoragePath(args[0])
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	s, err := resolver.Get(ctx, name)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	if err := s.Mkdir(ctx, dir); err != nil {
		if isCanceled(err) {
			return withExitCode(ExitInterrupted, fmt.Errorf("中断しました"))
		}
		return err
	}

	fmt.Printf("%s:%s を作りました。\n", name, dir)
	return nil
}

// splitStoragePath は "名前:パス" を分けます。
func splitStoragePath(arg string) (name, path string, err error) {
	parts := strings.SplitN(arg, ":", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("パスの記述が変です: %q（storage:path の形式で指定してください）", arg)
	}
	return parts[0], parts[1], nil
}
