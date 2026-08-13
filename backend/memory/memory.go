// Package memory はテスト用のインメモリなストレージです。
//
// 実際のファイルシステムやネットワークを使わないので、決定的で高速です。
// 転送エンジンのテストや、適合性テスト自体の検証に使います。
package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "memory"

type entry struct {
	data    []byte
	isDir   bool
	modTime time.Time
}

// Storage はインメモリのストレージです。
type Storage struct {
	name string

	mu sync.RWMutex
	// entries はパスをキーにした平坦な表です。
	// ディレクトリも1件として持ちます。
	entries map[string]*entry

	// hooks は障害を差し込むためのものです。
	hooks Hooks
}

// Hooks はテストで障害を再現するための差し込み口です。
type Hooks struct {
	// BeforeOp は各操作の前に呼ばれます。非 nil を返すとその操作は失敗します。
	// op は "list", "stat", "open", "put", "mkdir", "remove" のいずれかです。
	BeforeOp func(op, path string) error
	// PutReader は Put が読む Reader を差し替えます。
	// 途中で失敗する Reader を返すことで、転送の中断を再現できます。
	PutReader func(path string, r io.Reader) io.Reader
}

// New はインメモリのストレージを作ります。
func New(name string) *Storage {
	s := &Storage{
		name:    name,
		entries: map[string]*entry{},
	}
	s.entries["/"] = &entry{isDir: true, modTime: time.Now()}
	return s
}

// SetHooks は障害を差し込む設定を変更します。
func (s *Storage) SetHooks(h Hooks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks = h
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features はこのストレージにできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		ModTimePrecision: time.Nanosecond,
		CanSetModTime:    true,
		Hashes:           storage.HashSet{storage.SHA256, storage.MD5, storage.SHA1, storage.DropboxContent},
		ImplicitDirs:     true,
		EmptyDirs:        true,
		AtomicPut:        true,
	}
}

// Close はストレージを閉じます。
func (s *Storage) Close() error { return nil }

func clean(p string) string { return storage.CleanPath(p) }

func (s *Storage) wrapErr(op, p string, class storage.Class, err error) error {
	return storage.Wrap(op, s.name, p, class, err)
}

func (s *Storage) notFound(op, p string) error {
	return s.wrapErr(op, p, storage.ClassPermanent, fmt.Errorf("%w: %s", storage.ErrNotFound, p))
}

// check は ctx と差し込まれた障害を確認します。
func (s *Storage) check(ctx context.Context, op, p string) error {
	if err := ctx.Err(); err != nil {
		return s.wrapErr(op, p, storage.ClassCanceled, err)
	}

	s.mu.RLock()
	hook := s.hooks.BeforeOp
	s.mu.RUnlock()

	if hook != nil {
		if err := hook(op, p); err != nil {
			return err
		}
	}
	return nil
}

// List はディレクトリの中身を1件ずつ fn に渡します。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	dir = clean(dir)
	if err := s.check(ctx, "list", dir); err != nil {
		return err
	}

	s.mu.RLock()
	e, ok := s.entries[dir]
	if !ok {
		s.mu.RUnlock()
		return s.notFound("list", dir)
	}
	if !e.isDir {
		s.mu.RUnlock()
		return s.wrapErr("list", dir, storage.ClassPermanent, fmt.Errorf("%w: %s", storage.ErrNotDir, dir))
	}

	// 直下のものだけを集める
	infos := []storage.FileInfo{}
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	for p, child := range s.entries {
		if p == dir || !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := strings.TrimPrefix(p, prefix)
		if strings.Contains(rest, "/") {
			continue // 孫以降
		}
		infos = append(infos, s.infoLocked(p, child))
	}
	s.mu.RUnlock()

	// 順序を安定させる。実装によって順序が変わると
	// テストが不安定になるため。
	sort.Slice(infos, func(i, j int) bool { return infos[i].Path < infos[j].Path })

	for _, fi := range infos {
		if err := ctx.Err(); err != nil {
			return s.wrapErr("list", dir, storage.ClassCanceled, err)
		}
		if err := fn(fi); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) infoLocked(p string, e *entry) storage.FileInfo {
	fi := storage.FileInfo{
		Path:    p,
		Name:    path.Base(p),
		IsDir:   e.isDir,
		ModTime: e.modTime,
		Size:    int64(len(e.data)),
	}
	if e.isDir {
		fi.Size = storage.SizeUnknown
	}
	return fi
}

// Stat は1件のメタデータを返します。
func (s *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	p = clean(p)
	if err := s.check(ctx, "stat", p); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[p]
	if !ok {
		return nil, s.notFound("stat", p)
	}
	fi := s.infoLocked(p, e)
	return &fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	p = clean(p)
	if err := s.check(ctx, "open", p); err != nil {
		return nil, nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[p]
	if !ok {
		return nil, nil, s.notFound("open", p)
	}
	if e.isDir {
		return nil, nil, s.wrapErr("open", p, storage.ClassPermanent, fmt.Errorf("%w: %s", storage.ErrIsDir, p))
	}

	fi := s.infoLocked(p, e)
	data := make([]byte, len(e.data))
	copy(data, e.data)
	return io.NopCloser(bytes.NewReader(data)), &fi, nil
}

// OpenRange はファイルの一部を読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	rc, _, err := s.Open(ctx, p)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	data = data[offset:]
	if length >= 0 && length < int64(len(data)) {
		data = data[:length]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Put はファイルを書き込みます。
func (s *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	p = clean(p)
	if err := s.check(ctx, "put", p); err != nil {
		return nil, err
	}

	s.mu.RLock()
	wrap := s.hooks.PutReader
	s.mu.RUnlock()
	if wrap != nil {
		r = wrap(p, r)
	}

	// 宣言されたサイズではなく、読み終わるまでを書き込む。
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, &ctxReader{ctx: ctx, r: r}); err != nil {
		if ctx.Err() != nil {
			return nil, s.wrapErr("put", p, storage.ClassCanceled, err)
		}
		return nil, s.wrapErr("put", p, storage.ClassUnknown, err)
	}

	modTime := meta.ModTime
	if modTime.IsZero() {
		modTime = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mkdirAllLocked(path.Dir(p))
	s.entries[p] = &entry{data: buf.Bytes(), modTime: modTime}

	fi := s.infoLocked(p, s.entries[p])
	return &fi, nil
}

// ctxReader は読み取りのたびに ctx を確認します。
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

// Mkdir はディレクトリを作ります。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	dir = clean(dir)
	if err := s.check(ctx, "mkdir", dir); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mkdirAllLocked(dir)
	return nil
}

func (s *Storage) mkdirAllLocked(dir string) {
	dir = clean(dir)
	for d := dir; d != "/" && d != "."; d = path.Dir(d) {
		if e, ok := s.entries[d]; ok {
			if e.isDir {
				continue
			}
			// 同名のファイルがある場合は上書きしない
			return
		}
		s.entries[d] = &entry{isDir: true, modTime: time.Now()}
	}
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, p string) error {
	p = clean(p)
	if err := s.check(ctx, "remove", p); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[p]
	if !ok {
		return s.notFound("remove", p)
	}
	if e.isDir && s.hasChildrenLocked(p) {
		return s.wrapErr("remove", p, storage.ClassPermanent, fmt.Errorf("%w: %s", storage.ErrNotEmpty, p))
	}
	delete(s.entries, p)
	return nil
}

func (s *Storage) hasChildrenLocked(dir string) bool {
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	for p := range s.entries {
		if p != dir && strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	dir = clean(dir)
	if err := s.check(ctx, "purge", dir); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[dir]; !ok {
		return s.notFound("purge", dir)
	}

	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	for p := range s.entries {
		if p == dir || strings.HasPrefix(p, prefix) {
			delete(s.entries, p)
		}
	}
	return nil
}

// Move はファイルやディレクトリを移動します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	srcPath, dstPath = clean(srcPath), clean(dstPath)
	if err := s.check(ctx, "move", srcPath); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[srcPath]
	if !ok {
		return s.notFound("move", srcPath)
	}
	s.mkdirAllLocked(path.Dir(dstPath))
	s.entries[dstPath] = e
	delete(s.entries, srcPath)
	return nil
}

// SetModTime は最終更新時刻を変更します。
func (s *Storage) SetModTime(ctx context.Context, p string, t time.Time) error {
	p = clean(p)
	if err := s.check(ctx, "setmodtime", p); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[p]
	if !ok {
		return s.notFound("setmodtime", p)
	}
	e.modTime = t
	return nil
}

// Hash はファイルのハッシュを計算します。
func (s *Storage) Hash(ctx context.Context, p string, ht storage.HashType) (string, error) {
	rc, _, err := s.Open(ctx, p)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	h, err := storage.NewHash(ht)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(h, rc); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Snapshot は現在の内容をパスと中身の対応で返します。テストの検証用です。
func (s *Storage) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := map[string]string{}
	for p, e := range s.entries {
		if e.isDir {
			continue
		}
		out[p] = string(e.data)
	}
	return out
}

var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.Hasher      = (*Storage)(nil)
	_ storage.Mover       = (*Storage)(nil)
	_ storage.Purger      = (*Storage)(nil)
	_ storage.RangeOpener = (*Storage)(nil)
	_ storage.SetModTimer = (*Storage)(nil)
)
