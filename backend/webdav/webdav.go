// Package webdav は WebDAV を storage.Storage として実装します。
//
// Nextcloud や ownCloud のほか、Apache の mod_dav など一般の
// WebDAV サーバーに繋げます。
//
// 更新時刻の扱いだけは相手によって変わります。WebDAV には時刻を
// 書き換える標準の方法がなく、Nextcloud と ownCloud だけが
// 独自のヘッダを受け付けます。preset で使い分けてください。
package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "webdav"

// partSuffix は書き込み中のファイルに付ける印です。
const partSuffix = ".hbgpart"

// Storage は WebDAV です。
type Storage struct {
	name   string
	client *davClient
	root   string

	canSetModTime bool

	// knownDirs は「ある」と分かっているディレクトリです。
	//
	// WebDAV の PUT は親がないと 409 で断られます。かといって
	// 書き込みのたびに MKCOL を投げると要求が倍になり、同じ
	// ディレクトリへ並行して書くと衝突します。一度作った（あるいは
	// あると分かった）ものを覚えておけば、どちらも避けられます。
	dirsMu    sync.Mutex
	knownDirs map[string]struct{}
}

// New は WebDAV に接続します。
//
// ここでは通信しません。繋がるかどうかは最初の操作で分かります。
func New(_ context.Context, cfg Config) (*Storage, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("webdav %s: %w", cfg.Name, err)
	}

	client, err := newDavClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("webdav %s: %w", cfg.Name, err)
	}

	return &Storage{
		name:          cfg.Name,
		client:        client,
		root:          strings.Trim(cleanPath(cfg.Root), "/"),
		canSetModTime: cfg.canSetModTime(),
		knownDirs:     map[string]struct{}{},
	}, nil
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features は WebDAV にできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// getlastmodified は RFC1123 なので秒までです。
		ModTimePrecision: time.Second,
		// 一般の WebDAV サーバーでは更新時刻を保持できません。
		CanSetModTime:   s.canSetModTime,
		CaseInsensitive: false,
		Hashes:          nil,
		ImplicitDirs:    true,
		EmptyDirs:       true,
		// 別名で書いてから MOVE で置き換えます。
		AtomicPut: true,
	}
}

// Close はストレージを閉じます。
func (s *Storage) Close() error {
	s.client = nil
	return nil
}

// full は設定の起点を足した実際のパスを返します。
func (s *Storage) full(p string) string {
	p = strings.TrimPrefix(cleanPath(p), "/")
	if s.root == "" {
		return "/" + p
	}
	if p == "" {
		return "/" + s.root
	}
	return "/" + s.root + "/" + p
}

// cleanPath はパスを正規化します。
//
// "\" は区切りとして扱いません。WebDAV の名前に使えるふつうの
// 文字なので、区切りに読み替えると別の場所を指すことになります。
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

// List はディレクトリの直下を1件ずつ fn に渡します。
//
// PROPFIND に Depth: 1 を指定すると、そのディレクトリ自身と直下が
// まとめて返ります。自分自身は取り除きます。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	full := s.full(dir)

	entries, err := s.client.propfind(ctx, ensureSlash(full), 1)
	if err != nil {
		return s.wrapErr("list", dir, err)
	}

	base := cleanPath(dir)
	self := path.Base(full)

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return s.wrapErr("list", dir, err)
		}
		switch {
		case e.name == self && e.isDir:
			// 問い合わせたディレクトリ自身。
			continue
		case strings.HasSuffix(e.name, partSuffix):
			// 書き込み中のものは見せない。
			continue
		}
		if err := fn(e.info(base)); err != nil {
			return err
		}
	}
	return nil
}

// Stat は1件のメタデータを返します。
func (s *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	e, err := s.stat(ctx, p)
	if err != nil {
		return nil, s.wrapErr("stat", p, err)
	}

	cp := cleanPath(p)
	fi := e.info(path.Dir(cp))
	fi.Path, fi.Name = cp, path.Base(cp)
	if cp == "/" {
		fi.Name, fi.IsDir = "/", true
	}
	return &fi, nil
}

// stat は1件のメタデータを問い合わせます。
func (s *Storage) stat(ctx context.Context, p string) (davEntry, error) {
	entries, err := s.client.propfind(ctx, s.full(p), 0)
	if err != nil {
		return davEntry{}, err
	}
	if len(entries) == 0 {
		return davEntry{}, storage.ErrNotFound
	}
	return entries[0], nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	e, err := s.stat(ctx, p)
	if err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}
	if e.isDir {
		return nil, nil, s.wrapErr("open", p, storage.ErrIsDir)
	}

	rc, err := s.client.get(ctx, s.full(p), nil)
	if err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}

	cp := cleanPath(p)
	fi := e.info(path.Dir(cp))
	fi.Path, fi.Name = cp, path.Base(cp)

	return rc, &fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	rc, err := s.client.get(ctx, s.full(p), map[string]string{
		"Range": rangeHeader(offset, length),
	})
	if err != nil {
		return nil, s.wrapErr("open", p, err)
	}
	return rc, nil
}

func rangeHeader(offset, length int64) string {
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// Put はファイルを書き込みます。
//
// 別名で書いてから置き換えます。途中で止めても、中身の欠けた
// ファイルが本来の場所に残ることはありません。
// そのぶん、1ファイルにつき要求が1つ増えます。
func (s *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	cp := cleanPath(p)
	if cp == "/" {
		return nil, s.wrapErr("put", p, errors.New("起点をファイルとして書き込むことはできません"))
	}

	dst := s.full(p)
	tmp := tempPath(dst)

	counting := &countingReader{r: &ctxReader{ctx: ctx, r: r}}
	headers := mtimeHeaders(meta.ModTime, s.canSetModTime)

	if err := s.ensureDir(ctx, path.Dir(tmp)); err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	if err := s.client.put(ctx, tmp, counting, contentLength(meta), headers); err != nil {
		s.discard(ctx, tmp)
		return nil, s.wrapErr("put", p, err)
	}

	// 置き換え先があっても上書きする。
	if err := s.client.move(ctx, tmp, dst, true); err != nil {
		s.discard(ctx, tmp)
		return nil, s.wrapErr("put", p, err)
	}

	return &storage.FileInfo{
		Path:    cp,
		Name:    path.Base(cp),
		Size:    counting.n,
		ModTime: meta.ModTime,
	}, nil
}

// ensureDir は書き込み先のディレクトリを用意します。
//
// 一度確かめたものは覚えておくので、同じディレクトリへ続けて
// 書き込むあいだは要求が増えません。転送の側が先に Mkdir を
// 呼んでいれば、そこで覚えたぶんが効いて1度も増えません。
//
// 書き込んだあとで 409 を見てから作り直す、という順にはできません。
// 断られた時点で本文の送信が始まっていることがあり、読み手を
// 巻き戻せないためです。
func (s *Storage) ensureDir(ctx context.Context, dir string) error {
	if dir == "" || dir == "/" || s.dirIsKnown(dir) {
		return nil
	}

	// 同じディレクトリへ並行して書き込むとき、MKCOL がぶつからない
	// よう、ここは1つずつ行う。
	s.dirsMu.Lock()
	defer s.dirsMu.Unlock()

	if _, ok := s.knownDirs[dir]; ok {
		return nil
	}
	if err := s.mkdirAll(ctx, dir); err != nil {
		return err
	}
	s.knownDirs[dir] = struct{}{}
	return nil
}

func (s *Storage) dirIsKnown(dir string) bool {
	s.dirsMu.Lock()
	defer s.dirsMu.Unlock()
	_, ok := s.knownDirs[dir]
	return ok
}

// rememberDir は作ったディレクトリを覚えます。
func (s *Storage) rememberDir(dir string) {
	s.dirsMu.Lock()
	defer s.dirsMu.Unlock()
	s.knownDirs[dir] = struct{}{}
}

// forgetDir は消したディレクトリを忘れます。
func (s *Storage) forgetDir(dir string) {
	s.dirsMu.Lock()
	defer s.dirsMu.Unlock()

	delete(s.knownDirs, dir)
	prefix := strings.TrimSuffix(dir, "/") + "/"
	for k := range s.knownDirs {
		if strings.HasPrefix(k, prefix) {
			delete(s.knownDirs, k)
		}
	}
}

// contentLength は書き込みで伝える長さを決めます。
//
// 分かっている場合は伝えます。実際に送られた量と食い違えば
// その場で失敗するので、黙って切り詰められることはありません。
// 分からない場合は長さを伝えずに送ります。
func contentLength(meta storage.ObjectMeta) int64 {
	if meta.Size < 0 {
		return -1
	}
	return meta.Size
}

// discard は書きかけの一時ファイルを片付けます。
func (s *Storage) discard(ctx context.Context, tmp string) {
	// 取り消されていても片付けたいので、別の合図を使う。
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_ = s.client.remove(cleanupCtx, tmp)
}

// countingReader は読んだバイト数を数えます。
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
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

// tempPath は書き込み中の名前を組み立てます。
func tempPath(dst string) string {
	return path.Join(path.Dir(dst), "."+path.Base(dst)+partSuffix)
}

// Mkdir はディレクトリを（必要なら親ごと）作ります。
//
// MKCOL は親がないと 409 を返すので、そのときだけさかのぼります。
// すでにある場合は 405 が返るので、成功として扱います。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	if cleanPath(dir) == "/" {
		return nil
	}

	full := s.full(dir)
	if err := s.mkdirAll(ctx, full); err != nil {
		return s.wrapErr("mkdir", dir, err)
	}
	s.rememberDir(full)
	return nil
}

func (s *Storage) mkdirAll(ctx context.Context, full string) error {
	if full == "" || full == "/" {
		return nil
	}

	err := s.client.mkcol(ctx, full)
	switch {
	case err == nil:
		return nil
	case statusOfDav(err) == http.StatusMethodNotAllowed:
		// すでにある。
		return nil
	case statusOfDav(err) != http.StatusConflict:
		return err
	}

	// 親がまだない。
	if err = s.mkdirAll(ctx, path.Dir(full)); err != nil {
		return err
	}

	err = s.client.mkcol(ctx, full)
	if err != nil && statusOfDav(err) == http.StatusMethodNotAllowed {
		return nil
	}
	return err
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, p string) error {
	if cleanPath(p) == "/" {
		return s.wrapErr("remove", p, errors.New("起点は削除できません"))
	}

	e, err := s.stat(ctx, p)
	if err != nil {
		return s.wrapErr("remove", p, err)
	}

	// WebDAV の DELETE はディレクトリを中身ごと消す。
	// Remove の約束を守るため、空であることを先に確かめる。
	if e.isDir {
		empty, emptyErr := s.isEmpty(ctx, p)
		if emptyErr != nil {
			return s.wrapErr("remove", p, emptyErr)
		}
		if !empty {
			return s.wrapErr("remove", p,
				fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty))
		}
	}

	s.forgetDir(s.full(p))
	return s.wrapErr("remove", p, s.client.remove(ctx, s.full(p)))
}

// isEmpty はディレクトリが空かを返します。
func (s *Storage) isEmpty(ctx context.Context, dir string) (bool, error) {
	count := 0
	err := s.List(ctx, dir, func(storage.FileInfo) error {
		count++
		return nil
	})
	return count == 0, err
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if cleanPath(dir) == "/" {
		return s.wrapErr("purge", dir, errors.New("起点は削除できません"))
	}

	if _, err := s.stat(ctx, dir); err != nil {
		return s.wrapErr("purge", dir, err)
	}

	s.forgetDir(s.full(dir))
	return s.wrapErr("purge", dir, s.client.remove(ctx, ensureSlash(s.full(dir))))
}

// Move はサーバー側でファイルを移動・改名します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	if err := s.mkdirAll(ctx, path.Dir(s.full(dstPath))); err != nil {
		return s.wrapErr("move", dstPath, err)
	}
	return s.wrapErr("move", srcPath, s.client.move(ctx, s.full(srcPath), s.full(dstPath), true))
}

// ServerSideCopy は内容を転送せずにコピーします。
func (s *Storage) ServerSideCopy(ctx context.Context, srcPath, dstPath string) (*storage.FileInfo, error) {
	if err := s.mkdirAll(ctx, path.Dir(s.full(dstPath))); err != nil {
		return nil, s.wrapErr("copy", dstPath, err)
	}
	if err := s.client.copy(ctx, s.full(srcPath), s.full(dstPath), true); err != nil {
		return nil, s.wrapErr("copy", srcPath, err)
	}
	return s.Stat(ctx, dstPath)
}

// info は PROPFIND の結果を storage.FileInfo にします。
func (e davEntry) info(dir string) storage.FileInfo {
	fi := storage.FileInfo{
		Path:    path.Join(dir, e.name),
		Name:    e.name,
		IsDir:   e.isDir,
		Size:    e.size,
		ModTime: e.modTime,
	}
	if fi.IsDir || fi.Size < 0 {
		fi.Size = storage.SizeUnknown
	}
	return fi
}

var (
	_ storage.Storage          = (*Storage)(nil)
	_ storage.Purger           = (*Storage)(nil)
	_ storage.Mover            = (*Storage)(nil)
	_ storage.RangeOpener      = (*Storage)(nil)
	_ storage.ServerSideCopier = (*Storage)(nil)
)
