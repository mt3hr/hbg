package auth

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
)

// OpenBrowser は既定のブラウザで URL を開きます。
//
// 依存を増やさないよう、OS ごとの標準的なコマンドを直接呼びます。
func OpenBrowser(url string) error {
	// ブラウザは hbg より長く生きるので、実行に期限は設けない。
	ctx := context.Background()

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		// start は cmd の内部コマンド。第1引数はウィンドウタイトルとして
		// 解釈されるため、空文字を挟む必要がある。
		cmd = exec.CommandContext(ctx, "cmd", "/c", "start", "", url)
	case "darwin":
		cmd = exec.CommandContext(ctx, "open", url)
	default:
		cmd = exec.CommandContext(ctx, "xdg-open", url)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ブラウザを開けませんでした: %w", err)
	}
	// 終了は待たない。ブラウザが開いたままプロセスが残るため。
	go func() { _ = cmd.Wait() }()
	return nil
}
