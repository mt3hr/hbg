// Package local はローカルファイルシステムを storage.Storage として扱います。
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "local"

// partialSuffix は書き込み中の一時ファイルに付ける印です。
//
// 途中で中断したときに、壊れたファイルが正規の名前で残らないようにします。
// 以前は書き込み先へ直接 O_TRUNC で書いていたため、Ctrl-C で
// 中身が欠けたファイルがそのまま残っていました。
const partialSuffix = ".hbgpart"

// Storage はローカルファイルシステムです。
type Storage struct {
	name string
}

// New はローカルファイルシステムのストレージを作ります。
func New(name string) *Storage {
	return &Storage{name: name}
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features はこのストレージにできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// NTFS は 100ns、ext4 や APFS は 1ns。
		// FAT/exFAT は 2s だが、実行時に判別する手段がないため
		// 利用者に --update_duration で指定してもらう。
		ModTimePrecision: 100 * time.Nanosecond,
		CanSetModTime:    true,
		CaseInsensitive:  os.PathSeparator == '\\',
		Hashes:           storage.HashSet{storage.SHA256, storage.MD5, storage.SHA1, storage.DropboxContent},
		ImplicitDirs:     true,
		EmptyDirs:        true,
		AtomicPut:        true,
		OSPath:           true,
	}
}

// Close はストレージを閉じます。ローカルでは何もしません。
func (s *Storage) Close() error { return nil }

// osPath は storage のパスを OS のパスへ変換します。
func osPath(p string) string {
	return filepath.FromSlash(p)
}

// slashPath は OS のパスを storage のパスへ変換します。
func slashPath(p string) string {
	return filepath.ToSlash(p)
}

// wrapErr は os のエラーを storage のエラーに変換します。
func (s *Storage) wrapErr(op, path string, err error) error {
	if err == nil {
		return nil
	}

	class := storage.ClassUnknown
	switch {
	case errors.Is(err, os.ErrNotExist):
		err = fmt.Errorf("%w: %s", storage.ErrNotFound, path)
		class = storage.ClassPermanent
	case errors.Is(err, os.ErrPermission):
		class = storage.ClassPermanent
	case errors.Is(err, os.ErrExist):
		err = fmt.Errorf("%w: %s", storage.ErrExist, path)
		class = storage.ClassPermanent
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		class = storage.ClassCanceled
	}
	return storage.Wrap(op, s.name, path, class, err)
}

// List はディレクトリの中身を1件ずつ fn に渡します。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("list", dir, err)
	}

	entries, err := os.ReadDir(osPath(dir))
	if err != nil {
		return s.wrapErr("list", dir, err)
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return s.wrapErr("list", dir, err)
		}

		// DirEntry.Info はシンボリックリンクを辿らない。
		// 以前は os.Stat を別途呼んでおり、リンクを辿るうえに
		// 列挙中に消えたファイルで失敗していた。
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// 列挙してから読むまでの間に消えた場合は飛ばす
				continue
			}
			return s.wrapErr("list", filepath.Join(dir, entry.Name()), err)
		}

		fi := storage.FileInfo{
			Path:    slashPath(filepath.Join(osPath(dir), entry.Name())),
			Name:    entry.Name(),
			IsDir:   entry.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}
		if fi.IsDir {
			fi.Size = storage.SizeUnknown
		}
		if err := fn(fi); err != nil {
			return err
		}
	}
	return nil
}

// Stat は1件のメタデータを返します。
func (s *Storage) Stat(ctx context.Context, path string) (*storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("stat", path, err)
	}

	info, err := os.Stat(osPath(path))
	if err != nil {
		return nil, s.wrapErr("stat", path, err)
	}

	fi := &storage.FileInfo{
		Path:    slashPath(osPath(path)),
		Name:    info.Name(),
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if fi.IsDir {
		fi.Size = storage.SizeUnknown
	}
	return fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, path string) (io.ReadCloser, *storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, s.wrapErr("open", path, err)
	}

	f, err := os.Open(osPath(path))
	if err != nil {
		return nil, nil, s.wrapErr("open", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, s.wrapErr("open", path, err)
	}
	if info.IsDir() {
		f.Close()
		return nil, nil, s.wrapErr("open", path, fmt.Errorf("%w: %s", storage.ErrIsDir, path))
	}

	fi := &storage.FileInfo{
		Path:    slashPath(osPath(path)),
		Name:    info.Name(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	return &ctxReader{ctx: ctx, f: f}, fi, nil
}

// OpenRange はファイルの一部を読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("open", path, err)
	}

	f, err := os.Open(osPath(path))
	if err != nil {
		return nil, s.wrapErr("open", path, err)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, s.wrapErr("open", path, err)
	}

	var r io.Reader = f
	if length >= 0 {
		r = io.LimitReader(f, length)
	}
	return &ctxReader{ctx: ctx, f: f, r: r}, nil
}

// ctxReader は読み取りのたびに ctx を確認する ReadCloser です。
// これがないと、大きなファイルの読み取り中に Ctrl-C が効きません。
type ctxReader struct {
	ctx context.Context
	f   *os.File
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	if c.r != nil {
		return c.r.Read(p)
	}
	return c.f.Read(p)
}

func (c *ctxReader) Close() error { return c.f.Close() }

// Put はファイルを書き込みます。
//
// 一時ファイルへ書いてから所定の名前へ移すので、途中で中断しても
// 中身の欠けたファイルが残りません。
func (s *Storage) Put(ctx context.Context, path string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("put", path, err)
	}

	target := osPath(path)
	if err := os.MkdirAll(filepath.Dir(target), 0o777); err != nil {
		return nil, s.wrapErr("put", path, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(target), "."+filepath.Base(target)+".*"+partialSuffix)
	if err != nil {
		return nil, s.wrapErr("put", path, err)
	}
	tmpName := tmp.Name()

	// 失敗した場合に一時ファイルを残さない。
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	written, err := io.Copy(tmp, &ctxLimitReader{ctx: ctx, r: r})
	if err != nil {
		return nil, s.wrapErr("put", path, err)
	}
	if closeErr := tmp.Close(); closeErr != nil {
		return nil, s.wrapErr("put", path, closeErr)
	}

	// 更新時刻は移す前に設定する。移したあとだと、
	// 書き込み先によっては反映されないことがある。
	if !meta.ModTime.IsZero() {
		if timeErr := os.Chtimes(tmpName, meta.ModTime, meta.ModTime); timeErr != nil {
			return nil, s.wrapErr("put", path, timeErr)
		}
	}

	if renameErr := os.Rename(tmpName, target); renameErr != nil {
		return nil, s.wrapErr("put", path, renameErr)
	}
	committed = true

	info, err := os.Stat(target)
	if err != nil {
		return nil, s.wrapErr("put", path, err)
	}
	return &storage.FileInfo{
		Path:    slashPath(target),
		Name:    info.Name(),
		Size:    written,
		ModTime: info.ModTime(),
	}, nil
}

// ctxLimitReader は読み取りのたびに ctx を確認します。
type ctxLimitReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxLimitReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// Mkdir はディレクトリを作ります。すでにあれば何もしません。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("mkdir", dir, err)
	}
	// パーミッションには umask が適用される。
	// 以前は 0777 を指定しており、環境によっては誰でも書ける状態だった。
	if err := os.MkdirAll(osPath(dir), 0o777); err != nil {
		return s.wrapErr("mkdir", dir, err)
	}
	return nil
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("remove", path, err)
	}
	if err := os.Remove(osPath(path)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s.wrapErr("remove", path, err)
		}
		// ディレクトリが空でない場合
		if info, statErr := os.Stat(osPath(path)); statErr == nil && info.IsDir() {
			return s.wrapErr("remove", path, fmt.Errorf("%w: %s", storage.ErrNotEmpty, path))
		}
		return s.wrapErr("remove", path, err)
	}
	return nil
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("purge", dir, err)
	}
	if _, err := os.Stat(osPath(dir)); err != nil {
		return s.wrapErr("purge", dir, err)
	}
	if err := os.RemoveAll(osPath(dir)); err != nil {
		return s.wrapErr("purge", dir, err)
	}
	return nil
}

// Move はファイルやディレクトリを移動します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("move", srcPath, err)
	}
	if err := os.MkdirAll(filepath.Dir(osPath(dstPath)), 0o777); err != nil {
		return s.wrapErr("move", dstPath, err)
	}
	if err := os.Rename(osPath(srcPath), osPath(dstPath)); err != nil {
		return s.wrapErr("move", srcPath, err)
	}
	return nil
}

// SetModTime は最終更新時刻を変更します。
func (s *Storage) SetModTime(ctx context.Context, path string, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("setmodtime", path, err)
	}
	if err := os.Chtimes(osPath(path), t, t); err != nil {
		return s.wrapErr("setmodtime", path, err)
	}
	return nil
}

// Hash はファイルのハッシュを計算します。
func (s *Storage) Hash(ctx context.Context, path string, ht storage.HashType) (string, error) {
	h, err := storage.NewHash(ht)
	if err != nil {
		return "", err
	}

	f, err := os.Open(osPath(path))
	if err != nil {
		return "", s.wrapErr("hash", path, err)
	}
	defer f.Close()

	if _, err := io.Copy(h, &ctxLimitReader{ctx: ctx, r: f}); err != nil {
		return "", s.wrapErr("hash", path, err)
	}
	return hexString(h.Sum(nil)), nil
}

func hexString(b []byte) string {
	const digits = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(len(b) * 2)
	for _, c := range b {
		sb.WriteByte(digits[c>>4])
		sb.WriteByte(digits[c&0xf])
	}
	return sb.String()
}

// インターフェースを満たしていることをコンパイル時に確認する。
var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.Hasher      = (*Storage)(nil)
	_ storage.Mover       = (*Storage)(nil)
	_ storage.Purger      = (*Storage)(nil)
	_ storage.RangeOpener = (*Storage)(nil)
	_ storage.SetModTimer = (*Storage)(nil)
)
