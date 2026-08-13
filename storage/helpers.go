package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
)

// ここに並ぶヘルパは、オプショナルインターフェースへの型アサーションを
// 1箇所に閉じ込めるためのものです。呼び出し側は「できるなら速い方法、
// できないなら確実な方法」を意識せずに済みます。

// CanServerSideCopy は、src から dst へサーバー側コピーができるかを返します。
// 同じストレージ（種別と名前が一致）でなければ使えません。
func CanServerSideCopy(src, dst Storage) bool {
	if src.Type() != dst.Type() || src.Name() != dst.Name() {
		return false
	}
	_, ok := dst.(ServerSideCopier)
	return ok
}

// ServerSideCopy は内容を転送せずにコピーします。
// できない場合は ErrUnsupported を返します。
func ServerSideCopy(ctx context.Context, src, dst Storage, srcPath, dstPath string) (*FileInfo, error) {
	copier, ok := dst.(ServerSideCopier)
	if !ok || !CanServerSideCopy(src, dst) {
		return nil, fmt.Errorf("%w: サーバー側コピー", ErrUnsupported)
	}
	return copier.ServerSideCopy(ctx, srcPath, dstPath)
}

// Move は同じストレージ内でファイルを移動します。
// Mover を実装していない場合はコピーしてから削除します。
func Move(ctx context.Context, s Storage, srcPath, dstPath string) error {
	if mover, ok := s.(Mover); ok {
		return mover.Move(ctx, srcPath, dstPath)
	}

	// 読み取りを閉じてから削除する。
	// 開いたままだと Windows では「他のプロセスが使用中」で削除できない。
	if err := func() error {
		rc, info, err := s.Open(ctx, srcPath)
		if err != nil {
			return err
		}
		defer rc.Close()

		_, err = s.Put(ctx, dstPath, rc, ObjectMeta{
			Size:    info.Size,
			ModTime: info.ModTime,
			Hashes:  info.Hashes,
		})
		return err
	}(); err != nil {
		return err
	}

	return s.Remove(ctx, srcPath)
}

// PurgeAll はディレクトリを中身ごと削除します。
// Purger を実装していない場合は、後行順にたどって1件ずつ削除します。
func PurgeAll(ctx context.Context, s Storage, dir string) error {
	if purger, ok := s.(Purger); ok {
		return purger.Purge(ctx, dir)
	}

	// 子から先に消さないとディレクトリを削除できない。
	entries, err := ListAll(ctx, s, dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir {
			if err := PurgeAll(ctx, s, e.Path); err != nil {
				return err
			}
			continue
		}
		if err := s.Remove(ctx, e.Path); err != nil {
			return err
		}
	}
	return s.Remove(ctx, dir)
}

// ListAll はディレクトリの中身をすべて集めて返します。
//
// List はメモリを一定に保つためコールバック型ですが、
// 一覧表示のように全件が必要な場面ではこちらを使います。
func ListAll(ctx context.Context, s Storage, dir string) ([]FileInfo, error) {
	entries := []FileInfo{}
	err := s.List(ctx, dir, func(fi FileInfo) error {
		entries = append(entries, fi)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// ListAllSorted はディレクトリの中身を、ディレクトリを先にして名前順に返します。
func ListAllSorted(ctx context.Context, s Storage, dir string) ([]FileInfo, error) {
	entries, err := ListAll(ctx, s, dir)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})
	return entries, nil
}

// Exists はパスが存在するかを返します。
func Exists(ctx context.Context, s Storage, path string) (bool, error) {
	_, err := s.Stat(ctx, path)
	if err == nil {
		return true, nil
	}
	if IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// IsNotFound はエラーが「存在しない」を表すかを返します。
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// SetModTime は最終更新時刻を変更します。
// 対応していない場合は ErrUnsupported を返します。
func SetModTime(ctx context.Context, s Storage, path string, t time.Time) error {
	setter, ok := s.(SetModTimer)
	if !ok {
		return fmt.Errorf("%w: 更新時刻の変更", ErrUnsupported)
	}
	return setter.SetModTime(ctx, path, t)
}

// GetHash はファイルのハッシュを取得します。
//
// まず追加の入出力なしで得られるものを探し、なければ Hasher を使います。
// どちらもできない場合は ErrUnsupported を返します。
func GetHash(ctx context.Context, s Storage, info *FileInfo, ht HashType) (string, error) {
	if info != nil {
		if h, ok := info.Hashes[ht]; ok && h != "" {
			return h, nil
		}
	}

	hasher, ok := s.(Hasher)
	if !ok {
		return "", fmt.Errorf("%w: ハッシュ %s の取得", ErrUnsupported, ht)
	}
	p := ""
	if info != nil {
		p = info.Path
	}
	return hasher.Hash(ctx, p, ht)
}

// CopyOptions は Copy の動作を指定します。
type CopyOptions struct {
	// Wrap は読み取りに割り込む関数です。進捗の計測に使います。
	// nil なら何もしません。
	Wrap func(io.Reader) io.Reader
	// VerifyHash に種類を指定すると、転送しながら計算したハッシュと
	// 書き込み後のハッシュを突き合わせます。
	VerifyHash HashType
}

// Copy は src の1ファイルを dst へコピーします。
//
// 同じストレージ内でサーバー側コピーが使える場合はそちらを使い、
// 使えない場合は内容を読んで書き込みます。
func Copy(ctx context.Context, src Storage, srcPath string, dst Storage, dstPath string, opts CopyOptions) (*FileInfo, error) {
	if copier, ok := dst.(ServerSideCopier); ok && CanServerSideCopy(src, dst) {
		return copier.ServerSideCopy(ctx, srcPath, dstPath)
	}

	rc, info, err := src.Open(ctx, srcPath)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var r io.Reader = rc
	if opts.Wrap != nil {
		r = opts.Wrap(r)
	}

	var sum func() map[HashType]string
	if opts.VerifyHash != "" {
		w, getSum, hashErr := MultiHasher(opts.VerifyHash)
		if hashErr != nil {
			return nil, hashErr
		}
		sum = getSum
		r = io.TeeReader(r, w)
	}

	written, err := dst.Put(ctx, dstPath, r, ObjectMeta{
		Size:    info.Size,
		ModTime: info.ModTime,
		Hashes:  info.Hashes,
	})
	if err != nil {
		return nil, err
	}

	// 宣言されたサイズと実際に書かれたサイズの食い違いは、
	// 内容が切り詰められたことを意味する。必ず検査する。
	if info.Size != SizeUnknown && written.Size != SizeUnknown && written.Size != info.Size {
		return nil, fmt.Errorf("転送したサイズが一致しません（元 %d バイト、先 %d バイト）: %s",
			info.Size, written.Size, dstPath)
	}

	if sum != nil {
		if err := verifyHash(sum(), written, opts.VerifyHash, dstPath); err != nil {
			return nil, err
		}
	}
	return written, nil
}

func verifyHash(computed map[HashType]string, written *FileInfo, ht HashType, dstPath string) error {
	want, ok := computed[ht]
	if !ok || want == "" {
		return nil
	}
	got, ok := written.Hashes[ht]
	if !ok || got == "" {
		// 書き込み先がハッシュを返さない場合は検証できない。
		return nil
	}
	if !strings.EqualFold(want, got) {
		return fmt.Errorf("転送内容のハッシュが一致しません（%s: 元 %s、先 %s）: %s", ht, want, got, dstPath)
	}
	return nil
}

// CleanPath はストレージのルートを起点としたパスを正規化します。
// 区切りは "/" で、先頭には "/" を補います。
//
// "\\" は区切りとして扱いません。多くのクラウドストレージでは
// ファイル名に含められるふつうの文字なので、区切りに読み替えると
// "a\b.txt" という名前のファイルが "a" の下の "b.txt" になってしまいます。
//
// ローカルファイルシステムのように OS のパス規則に従うストレージ
// （Features.OSPath が真）には使わないでください。
// ドライブレターが C:/... から /C:/... に壊れてしまいます。
func CleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}
