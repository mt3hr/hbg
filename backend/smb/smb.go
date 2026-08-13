// Package smb は SMB（Windows のファイル共有・Samba）を
// storage.Storage として実装します。
//
// Windows で共有をドライブに割り当てている場合は、local として
// そのドライブを指すほうが速くて確実です。このバックエンドは、
// 割り当てずに直接繋ぎたい場合や、Windows 以外から使う場合のものです。
package smb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "smb"

// partSuffix は書き込み中のファイルに付ける印です。
const partSuffix = ".hbgpart"

// dirPerm と filePerm は作るときの権限です。
// SMB では実際には使われませんが、ファイル操作の形に合わせて渡します。
const (
	dirPerm  os.FileMode = 0o755
	filePerm os.FileMode = 0o644
)

// Storage は SMB の共有です。
type Storage struct {
	name string
	fs   fileSystem
	root string
}

// New は共有に接続します。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	fs, err := connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("smb %s: %w", cfg.Name, err)
	}

	return &Storage{
		name: cfg.Name,
		fs:   fs,
		root: strings.Trim(cleanPath(cfg.Root), "/"),
	}, nil
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features は SMB にできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// SMB の時刻は 100ナノ秒きざみです。
		ModTimePrecision: 100 * time.Nanosecond,
		CanSetModTime:    true,
		// Windows の共有は大文字小文字を区別しません。
		CaseInsensitive: true,
		// 内容のハッシュを求める方法がありません。
		Hashes:       nil,
		ImplicitDirs: true,
		EmptyDirs:    true,
		// 別名で書いてから置き換えます。
		AtomicPut: true,
		// Windows のファイル名に使えない文字。
		IllegalChars: `<>:"|?*`,
	}
}

// Close は接続を閉じます。
func (s *Storage) Close() error {
	if s.fs == nil {
		return nil
	}
	err := s.fs.Close()
	s.fs = nil
	return err
}

// with は取り消しの合図を伝えたファイル操作を返します。
func (s *Storage) with(ctx context.Context) fileSystem {
	return s.fs.WithContext(ctx)
}

// full は設定の起点を足した実際のパスを返します。
func (s *Storage) full(p string) string {
	p = strings.TrimPrefix(cleanPath(p), "/")
	if s.root == "" {
		return p
	}
	if p == "" {
		return s.root
	}
	return s.root + "/" + p
}

// cleanPath はパスを正規化します。
//
// 共有の中では "/" を区切りとして扱います。"\" は Windows の
// ファイル名に使えない文字なので、ここでは区切りに読み替えます。
// SMB を使う相手は Windows の作法に従うためです。
func cleanPath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

// List はディレクトリの直下を1件ずつ fn に渡します。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("list", dir, err)
	}

	base := cleanPath(dir)
	entries, err := s.with(ctx).ReadDir(s.full(dir))
	if err != nil {
		return s.wrapErr("list", dir, err)
	}

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return s.wrapErr("list", dir, err)
		}
		if strings.HasSuffix(e.Name(), partSuffix) {
			// 書き込み中のものは見せない。
			continue
		}
		if err := fn(toFileInfo(e, base)); err != nil {
			return err
		}
	}
	return nil
}

// Stat は1件のメタデータを返します。
func (s *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("stat", p, err)
	}

	info, err := s.with(ctx).Stat(s.full(p))
	if err != nil {
		return nil, s.wrapErr("stat", p, err)
	}

	fi := toFileInfo(info, path.Dir(cleanPath(p)))
	return &fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}

	f, err := s.with(ctx).Open(s.full(p))
	if err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, s.wrapErr("open", p, err)
	}
	if info.IsDir() {
		f.Close()
		return nil, nil, s.wrapErr("open", p, storage.ErrIsDir)
	}

	cp := cleanPath(p)
	fi := toFileInfo(info, path.Dir(cp))
	fi.Path, fi.Name = cp, path.Base(cp)

	return &ctxReadCloser{ctx: ctx, rc: f}, &fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("open", p, err)
	}

	f, err := s.with(ctx).Open(s.full(p))
	if err != nil {
		return nil, s.wrapErr("open", p, err)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, s.wrapErr("open", p, err)
	}

	var r io.Reader = f
	if length >= 0 {
		r = io.LimitReader(f, length)
	}
	return &ctxReadCloser{ctx: ctx, rc: readCloser{Reader: r, Closer: f}}, nil
}

// readCloser は読み手と閉じ手を組み合わせます。
type readCloser struct {
	io.Reader
	io.Closer
}

// ctxReadCloser は読み取りのたびに取り消しを確かめます。
type ctxReadCloser struct {
	ctx context.Context
	rc  io.ReadCloser
}

func (c *ctxReadCloser) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.rc.Read(p)
}

func (c *ctxReadCloser) Close() error { return c.rc.Close() }

// Put はファイルを書き込みます。
//
// 別名で書いてから置き換えます。途中で止めても、中身の欠けた
// ファイルが本来の場所に残ることはありません。
func (s *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	fs := s.with(ctx)
	dst := s.full(p)
	tmp := tempPath(dst)

	f, err := s.createTemp(fs, tmp)
	if err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	written, err := writeAndClose(ctx, f, r)
	if err != nil {
		// 書きかけを残さない。
		_ = fs.Remove(tmp)
		return nil, s.wrapErr("put", p, err)
	}

	if !meta.ModTime.IsZero() {
		// 置き換える前に時刻を合わせる。あとから変えると、
		// 失敗したときに時刻だけ違うファイルが残る。
		if err := fs.Chtimes(tmp, meta.ModTime, meta.ModTime); err != nil {
			_ = fs.Remove(tmp)
			return nil, s.wrapErr("put", p, err)
		}
	}

	if err := s.replace(fs, tmp, dst); err != nil {
		_ = fs.Remove(tmp)
		return nil, s.wrapErr("put", p, err)
	}

	cp := cleanPath(p)
	return &storage.FileInfo{
		Path:    cp,
		Name:    path.Base(cp),
		Size:    written,
		ModTime: meta.ModTime,
	}, nil
}

// createTemp は書き込み用の一時ファイルを作ります。
// 親ディレクトリがなければ作ってから作り直します。
func (s *Storage) createTemp(fs fileSystem, tmp string) (file, error) {
	f, err := fs.Create(tmp)
	if err == nil {
		return f, nil
	}
	if !isNotExist(err) {
		return nil, err
	}

	if err := mkdirAll(fs, path.Dir(tmp)); err != nil {
		return nil, err
	}
	return fs.Create(tmp)
}

// writeAndClose は内容を書ききって閉じます。書いた長さを返します。
func writeAndClose(ctx context.Context, f file, r io.Reader) (int64, error) {
	n, err := io.Copy(f, &ctxReader{ctx: ctx, r: r})
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return n, err
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// replace は一時ファイルを本来の場所へ移します。
//
// SMB の改名は、置き換え先があると失敗します。先にどけるので、
// ごく短い間ですが置き換え先が存在しない時間ができます。
func (s *Storage) replace(fs fileSystem, tmp, dst string) error {
	if err := fs.Rename(tmp, dst); err == nil {
		return nil
	}

	if err := fs.Remove(dst); err != nil && !isNotExist(err) {
		return err
	}
	return fs.Rename(tmp, dst)
}

// tempPath は書き込み中の名前を組み立てます。
func tempPath(dst string) string {
	return path.Join(path.Dir(dst), "."+path.Base(dst)+partSuffix)
}

// Mkdir はディレクトリを（必要なら親ごと）作ります。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("mkdir", dir, err)
	}
	return s.wrapErr("mkdir", dir, mkdirAll(s.with(ctx), s.full(dir)))
}

// mkdirAll は親をさかのぼって作ります。
func mkdirAll(fs fileSystem, dir string) error {
	if dir == "" || dir == "." || dir == "/" {
		return nil
	}

	info, err := fs.Stat(dir)
	switch {
	case err == nil && info.IsDir():
		return nil
	case err == nil:
		return fmt.Errorf("%w: %s は既にファイルです", storage.ErrExist, dir)
	case !isNotExist(err):
		return err
	}

	if err := mkdirAll(fs, path.Dir(dir)); err != nil {
		return err
	}

	if err := fs.Mkdir(dir, dirPerm); err != nil {
		// 並行して作られた場合に備える。
		if info, statErr := fs.Stat(dir); statErr == nil && info.IsDir() {
			return nil
		}
		return err
	}
	return nil
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("remove", p, err)
	}
	if cleanPath(p) == "/" {
		return s.wrapErr("remove", p, errors.New("共有の起点は削除できません"))
	}

	fs := s.with(ctx)
	full := s.full(p)

	// 空でないディレクトリを消してしまわないよう、先に確かめる。
	if info, err := fs.Stat(full); err == nil && info.IsDir() {
		entries, listErr := fs.ReadDir(full)
		if listErr != nil {
			return s.wrapErr("remove", p, listErr)
		}
		if len(entries) > 0 {
			return s.wrapErr("remove", p,
				fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty))
		}
	}

	return s.wrapErr("remove", p, fs.Remove(full))
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("purge", dir, err)
	}
	if cleanPath(dir) == "/" {
		return s.wrapErr("purge", dir, errors.New("共有の起点は削除できません"))
	}
	return s.wrapErr("purge", dir, removeAll(ctx, s.with(ctx), s.full(dir)))
}

// removeAll は中身をたどって消します。
func removeAll(ctx context.Context, fs fileSystem, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	info, err := fs.Stat(target)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fs.Remove(target)
	}

	entries, err := fs.ReadDir(target)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := removeAll(ctx, fs, path.Join(target, e.Name())); err != nil {
			return err
		}
	}
	return fs.Remove(target)
}

// Move は共有の中でファイルを移動・改名します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("move", srcPath, err)
	}

	fs := s.with(ctx)
	dst := s.full(dstPath)
	if err := mkdirAll(fs, path.Dir(dst)); err != nil {
		return s.wrapErr("move", dstPath, err)
	}
	return s.wrapErr("move", srcPath, s.replace(fs, s.full(srcPath), dst))
}

// SetModTime は最終更新時刻を変えます。
func (s *Storage) SetModTime(ctx context.Context, p string, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("setmodtime", p, err)
	}
	return s.wrapErr("setmodtime", p, s.with(ctx).Chtimes(s.full(p), t, t))
}

// toFileInfo は os.FileInfo を storage.FileInfo にします。
func toFileInfo(info os.FileInfo, dir string) storage.FileInfo {
	fi := storage.FileInfo{
		Path:    path.Join(dir, info.Name()),
		Name:    info.Name(),
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if fi.IsDir {
		fi.Size = storage.SizeUnknown
	}
	return fi
}

var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.Purger      = (*Storage)(nil)
	_ storage.Mover       = (*Storage)(nil)
	_ storage.RangeOpener = (*Storage)(nil)
	_ storage.SetModTimer = (*Storage)(nil)
)
