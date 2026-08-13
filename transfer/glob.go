package transfer

import (
	"context"
	"fmt"
	"path"
	"strings"

	glb "github.com/gobwas/glob"

	"github.com/mt3hr/hbg/storage"
)

// hasGlobMeta は、パスにワイルドカードが含まれるかを返します。
//
// 判定はコピー元のパスの最後の要素だけで行います。途中のディレクトリに
// 書けるようにすると、どこから一覧すればよいかが決まらないためです。
func hasGlobMeta(srcPath string) bool {
	return strings.ContainsAny(path.Base(srcPath), "*?[{")
}

// resolveSources は、コピー元のパスから転送の起点を返します。
//
// ワイルドカードを含まない場合は、そのパス1つだけを返します。
// 含む場合は親ディレクトリを一覧し、パターンに一致したものを返します。
//
//	hbg copy dropbox:/photos/* local:/backup    photos の中身を backup 直下へ
//	hbg copy dropbox:/configs/*.db local:/conf  .db だけを conf 直下へ
//
// 一致するものが1つも無ければエラーにします。0件を成功にすると、
// パスやパターンの打ち間違いがスクリプトからは成功に見えてしまいます。
func resolveSources(ctx context.Context, src storage.Storage, srcPath string) ([]storage.FileInfo, error) {
	if !hasGlobMeta(srcPath) {
		info, err := src.Stat(ctx, srcPath)
		if err != nil {
			return nil, fmt.Errorf("コピー元を確認できません %s:%s: %w", src.Type(), srcPath, err)
		}
		return []storage.FileInfo{*info}, nil
	}

	pattern, err := glb.Compile(srcPath)
	if err != nil {
		return nil, fmt.Errorf("コピー元のパターンを解釈できません %s:%s: %w", src.Type(), srcPath, err)
	}

	parent := path.Dir(srcPath)
	entries, err := storage.ListAll(ctx, src, parent)
	if err != nil {
		return nil, fmt.Errorf("コピー元を一覧できません %s:%s: %w", src.Type(), parent, err)
	}

	matched := []storage.FileInfo{}
	for _, entry := range entries {
		if pattern.Match(entry.Path) {
			matched = append(matched, entry)
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("コピー元に一致するものがありません %s:%s", src.Type(), srcPath)
	}
	return matched, nil
}

// checkGlobSupported は、削除を伴う転送でのワイルドカードを断ります。
//
// ワイルドカードのときは一致した起点しか走査しないので、コピー先の
// それ以外の中身は「コピー元に無いもの」として扱いようがありません。
// 消せるかのような顔をして消さないより、断ったほうが安全です。
func checkGlobSupported(opts Options) error {
	if opts.Delete && hasGlobMeta(opts.SrcPath) {
		return fmt.Errorf("コピー元にワイルドカードを書いた転送では削除できません: %s:%s",
			opts.Src.Type(), opts.SrcPath)
	}
	return nil
}
