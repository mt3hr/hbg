// Package sftp は SFTP を storage.Storage として実装します。
//
// SSH さえ通っていれば使えるので、NAS や自前のサーバーへの
// バックアップ先として使えます。
package sftp

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
	sftpc "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Type はこのバックエンドの種別名です。
const Type = "sftp"

// partSuffix は書き込み中のファイルに付ける印です。
const partSuffix = ".hbgpart"

// maxConcurrentRequests は1ファイルに対する同時要求の数です。
//
// SSH の1つの経路で読み書きするため、要求を並べないと
// 往復の待ち時間で速度が頭打ちになります。
const maxConcurrentRequests = 64

// Storage は SFTP です。
type Storage struct {
	name   string
	conn   *ssh.Client
	client *sftpc.Client
	root   string
}

// New は SFTP に接続します。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	conn, err := dial(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("sftp %s: %w", cfg.Name, err)
	}

	client, err := newSFTPClient(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("sftp %s: %w", cfg.Name, err)
	}

	return &Storage{
		name:   cfg.Name,
		conn:   conn,
		client: client,
		root:   cfg.Root,
	}, nil
}

// newSFTPClient は SSH の接続の上に SFTP のやりとりを乗せます。
func newSFTPClient(conn *ssh.Client) (*sftpc.Client, error) {
	return sftpc.NewClient(conn,
		sftpc.MaxConcurrentRequestsPerFile(maxConcurrentRequests),
		sftpc.UseConcurrentReads(true),
		// 並行して書くと速いが、途中で失敗したファイルに穴が空きうる。
		// hbg は別名で書いてから置き換えるので、穴の空いたものが
		// 最終的な場所に残ることはない。
		sftpc.UseConcurrentWrites(true),
	)
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features は SFTP にできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// SFTP version 3 の時刻は秒までです。
		ModTimePrecision: time.Second,
		CanSetModTime:    true,
		CaseInsensitive:  false,
		// 内容のハッシュを求める標準の方法がありません。
		Hashes:       nil,
		ImplicitDirs: true,
		EmptyDirs:    true,
		// 別名で書いてから置き換えます。
		AtomicPut: true,
	}
}

// Close は接続を閉じます。
func (s *Storage) Close() error {
	if s.client == nil {
		return nil
	}
	err := s.client.Close()
	if connErr := s.conn.Close(); err == nil {
		err = connErr
	}
	s.client, s.conn = nil, nil
	return err
}

// full は設定の起点を足した実際のパスを返します。
func (s *Storage) full(p string) string {
	return joinRoot(s.root, p)
}

// List はディレクトリの直下を1件ずつ fn に渡します。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("list", dir, err)
	}

	base := cleanPath(dir)
	entries, err := s.client.ReadDirContext(ctx, s.full(dir))
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

	info, err := s.client.Stat(s.full(p))
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

	f, err := s.client.Open(s.full(p))
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

	fi := toFileInfo(info, path.Dir(cleanPath(p)))
	fi.Name = path.Base(cleanPath(p))
	fi.Path = cleanPath(p)

	return &ctxReadCloser{ctx: ctx, rc: f}, &fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("open", p, err)
	}

	f, err := s.client.Open(s.full(p))
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

	dst := s.full(p)
	tmp := tempPath(dst)

	f, err := s.createTemp(tmp)
	if err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	written, err := s.writeAndClose(ctx, f, r)
	if err != nil {
		// 書きかけを残さない。
		_ = s.client.Remove(tmp)
		return nil, s.wrapErr("put", p, err)
	}

	if !meta.ModTime.IsZero() {
		// 置き換える前に時刻を合わせる。あとから変えると、
		// 失敗したときに時刻だけ違うファイルが残る。
		if err := s.client.Chtimes(tmp, meta.ModTime, meta.ModTime); err != nil {
			_ = s.client.Remove(tmp)
			return nil, s.wrapErr("put", p, err)
		}
	}

	if err := s.replace(tmp, dst); err != nil {
		_ = s.client.Remove(tmp)
		return nil, s.wrapErr("put", p, err)
	}

	return &storage.FileInfo{
		Path:    cleanPath(p),
		Name:    path.Base(cleanPath(p)),
		Size:    written,
		ModTime: meta.ModTime,
	}, nil
}

// createTemp は書き込み用の一時ファイルを作ります。
// 親ディレクトリがなければ作ってから作り直します。
func (s *Storage) createTemp(tmp string) (*sftpc.File, error) {
	f, err := s.client.Create(tmp)
	if err == nil {
		return f, nil
	}
	if !isNotExist(err) {
		return nil, err
	}

	// 親がまだない。作ってからやり直す。
	if err := s.client.MkdirAll(path.Dir(tmp)); err != nil {
		return nil, err
	}
	return s.client.Create(tmp)
}

// writeAndClose は内容を書ききって閉じます。書いた長さを返します。
func (s *Storage) writeAndClose(ctx context.Context, f *sftpc.File, r io.Reader) (int64, error) {
	// ReadFrom は要求を並べて送るので、往復の待ち時間に埋もれません。
	n, err := f.ReadFrom(&ctxReader{ctx: ctx, r: r})
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
func (s *Storage) replace(tmp, dst string) error {
	// posix-rename@openssh.com があれば、置き換えは不可分に行われる。
	if err := s.client.PosixRename(tmp, dst); err == nil {
		return err
	}

	// 拡張がないサーバー向け。すでにあるものをどけてから移す。
	// ここだけは一瞬だが、置き換え先が存在しない時間ができる。
	if err := s.client.Remove(dst); err != nil && !isNotExist(err) {
		return err
	}
	return s.client.Rename(tmp, dst)
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
	return s.wrapErr("mkdir", dir, s.client.MkdirAll(s.full(dir)))
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("remove", p, err)
	}

	err := s.client.Remove(s.full(p))
	if err == nil {
		return nil
	}

	// SFTP version 3 には「空でない」を表す番号がないので、
	// サーバーは単なる失敗として返してくる。何が起きたのかを
	// 利用者に伝えられるよう、ここで確かめ直す。
	return s.wrapErr("remove", p, s.explainRemoveFailure(ctx, p, err))
}

// explainRemoveFailure は削除の失敗の理由を突き止めます。
func (s *Storage) explainRemoveFailure(ctx context.Context, p string, err error) error {
	if isNotExist(err) {
		return err
	}

	info, statErr := s.client.Stat(s.full(p))
	if statErr != nil || !info.IsDir() {
		return err
	}

	entries, listErr := s.client.ReadDirContext(ctx, s.full(p))
	if listErr == nil && len(entries) > 0 {
		return fmt.Errorf("%w: 中身ごと消すには purge を使ってください（%w）",
			storage.ErrNotEmpty, err)
	}
	return err
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("purge", dir, err)
	}
	return s.wrapErr("purge", dir, s.client.RemoveAll(s.full(dir)))
}

// Move は同じサーバーの中でファイルを移動・改名します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("move", srcPath, err)
	}

	dst := s.full(dstPath)
	if err := s.client.MkdirAll(path.Dir(dst)); err != nil {
		return s.wrapErr("move", dstPath, err)
	}
	return s.wrapErr("move", srcPath, s.replace(s.full(srcPath), dst))
}

// SetModTime は最終更新時刻を変えます。
func (s *Storage) SetModTime(ctx context.Context, p string, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr("setmodtime", p, err)
	}
	return s.wrapErr("setmodtime", p, s.client.Chtimes(s.full(p), t, t))
}

// --- パスとメタデータ ---

// cleanPath はパスを正規化します。
//
// SFTP のパスは POSIX と同じ形です。"\" は区切りではなく
// ファイル名の一部として扱います。
// 相対のパスはそのまま渡します。サーバー側でログイン時の
// ディレクトリを起点として解決されます。
func cleanPath(p string) string {
	if p == "" {
		return "."
	}
	return path.Clean(p)
}

// joinRoot は設定の起点とパスを繋げます。
func joinRoot(root, p string) string {
	p = cleanPath(p)
	if root == "" {
		return p
	}
	if path.IsAbs(p) {
		// 起点を指定していても、絶対パスはそのまま使う。
		return p
	}
	return path.Join(root, p)
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

// isNotExist はエラーが「存在しない」を表すかを返します。
func isNotExist(err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var status *sftpc.StatusError
	if errors.As(err, &status) {
		return status.Code == uint32(sftpc.ErrSSHFxNoSuchFile)
	}
	return false
}

var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.Purger      = (*Storage)(nil)
	_ storage.Mover       = (*Storage)(nil)
	_ storage.RangeOpener = (*Storage)(nil)
	_ storage.SetModTimer = (*Storage)(nil)
)
