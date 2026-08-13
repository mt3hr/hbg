// Package ftp は FTP を storage.Storage として実装します。
//
// 古い NAS など、FTP しか話せない相手のためのものです。
// 既定では AUTH TLS で暗号化します。
package ftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	ftpc "github.com/jlaffaye/ftp"
	"github.com/mt3hr/hbg/internal/dircache"
	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "ftp"

// partSuffix は書き込み中のファイルに付ける印です。
const partSuffix = ".hbgpart"

// defaultPort は FTP の既定のポートです。
const defaultPort = 21

// defaultMaxConns は同時に開く接続の既定の上限です。
const defaultMaxConns = 4

// 暗号化の仕方。
const (
	// TLSExplicit は AUTH TLS で暗号化に切り替えます。既定です。
	TLSExplicit = "explicit"
	// TLSImplicit ははじめから暗号化された通信路で繋ぎます。
	TLSImplicit = "implicit"
	// TLSNone は暗号化しません。合言葉が平文で流れます。
	TLSNone = "none"
)

// Config は FTP ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。
	Name string

	// Host は接続先です。
	Host string
	// Port は接続先のポートです。0 なら 21 です。
	Port int
	// User はログイン名です。省略すると anonymous です。
	User string
	// Password は合言葉です。
	// 設定ファイルへの直接記述は避け、${環境変数} での指定を推奨します。
	Password string

	// TLS は暗号化の仕方です。
	// "explicit"（既定）、"implicit"、"none" のいずれかです。
	TLS string
	// InsecureSkipVerify を真にすると、証明書を確かめません。
	// 自己署名の証明書を使っている相手向けです。
	InsecureSkipVerify bool

	// DisableEPSV を真にすると、拡張の受け身の接続を使いません。
	// 古いサーバー向けです。
	DisableEPSV bool
	// DisableMLSD を真にすると、機械向けの一覧を使いません。
	// 一覧の形が壊れているサーバー向けです。
	DisableMLSD bool

	// MaxConns は同時に開く接続の上限です。0 なら 4 です。
	MaxConns int

	// Root を指定すると、その下を起点として扱います。
	Root string
}

func (c Config) port() int {
	if c.Port == 0 {
		return defaultPort
	}
	return c.Port
}

func (c Config) user() string {
	if c.User == "" {
		return "anonymous"
	}
	return c.User
}

func (c Config) password() string {
	if c.User == "" && c.Password == "" {
		// 匿名での接続では、慣習として連絡先を入れます。
		return "hbg@example.invalid"
	}
	return c.Password
}

func (c Config) tls() string {
	if c.TLS == "" {
		return TLSExplicit
	}
	return c.TLS
}

// validate は接続を試みる前に設定の不足を知らせます。
func (c Config) validate() error {
	if c.Host == "" {
		return errors.New("接続先（host）が指定されていません")
	}

	switch c.tls() {
	case TLSExplicit, TLSImplicit, TLSNone:
	default:
		return fmt.Errorf("tls には %q, %q, %q のいずれかを指定してください（%q が指定されました）",
			TLSExplicit, TLSImplicit, TLSNone, c.TLS)
	}
	return nil
}

// Storage は FTP です。
type Storage struct {
	name string
	pool *connPool
	root string

	// canSetModTime は相手が更新時刻の書き換えに応じるかどうかです。
	// 最初の接続で分かります。
	canSetModTime bool

	// dirs は用意済みのディレクトリの記憶です。
	// 書き込みのたびに親を作りにいかずに済ませるためのものです。
	dirs dircache.Cache

	// notify は利用者への通知です。nil なら何もしません。
	notify func(string)
}

// New は FTP に接続します。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("ftp %s: %w", cfg.Name, err)
	}

	maxConns := cfg.MaxConns
	if maxConns < 1 {
		maxConns = defaultMaxConns
	}
	pool := newConnPool(cfg, maxConns)

	// できることを確かめるために1つ繋いでおく。
	conn, err := pool.get(ctx)
	if err != nil {
		return nil, fmt.Errorf("ftp %s: %w", cfg.Name, err)
	}
	canSetModTime := conn.IsSetTimeSupported()
	pool.put(conn)

	return &Storage{
		name:          cfg.Name,
		pool:          pool,
		root:          strings.Trim(cleanPath(cfg.Root), "/"),
		canSetModTime: canSetModTime,
	}, nil
}

// SetNotifier は利用者への通知の宛先を決めます。
func (s *Storage) SetNotifier(fn func(string)) { s.notify = fn }

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features は FTP にできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// MLSD も MFMT も秒までです。
		ModTimePrecision: time.Second,
		// MFMT に応じる相手なら保持できます。
		//
		// 以前の実装は時刻をまったく扱っておらず、ヘルプにも
		// 「タイムスタンプは消滅します」と書かれていました。
		CanSetModTime:   s.canSetModTime,
		CaseInsensitive: false,
		Hashes:          nil,
		ImplicitDirs:    true,
		EmptyDirs:       true,
		// 別名で書いてから置き換えます。
		AtomicPut: true,
	}
}

// Close はすべての接続を閉じます。
func (s *Storage) Close() error {
	if s.pool == nil {
		return nil
	}
	err := s.pool.close()
	s.pool = nil
	return err
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
// "\" は区切りとして扱いません。FTP の名前に使えるふつうの
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

// withConn は接続を1つ借りて fn を実行します。
func (s *Storage) withConn(ctx context.Context, fn func(*ftpc.ServerConn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	conn, err := s.pool.get(ctx)
	if err != nil {
		return err
	}

	err = fn(conn)
	if isConnectionBroken(err) {
		// 壊れた接続を使い回すと、次の操作まで巻き添えになる。
		s.pool.discard(conn)
		return err
	}
	s.pool.put(conn)
	return err
}

// List はディレクトリの直下を1件ずつ fn に渡します。
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	base := cleanPath(dir)
	full := s.full(dir)

	var entries []*ftpc.Entry
	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		var listErr error
		entries, listErr = conn.List(full)
		return listErr
	})
	if err != nil {
		return s.wrapErr("list", dir, err)
	}

	// 一覧に自分自身と親が混ざる形式のサーバーがある。
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return s.wrapErr("list", dir, err)
		}
		switch e.Name {
		case ".", "..", "":
			continue
		}
		if strings.HasSuffix(e.Name, partSuffix) {
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
	cp := cleanPath(p)
	if cp == "/" {
		return &storage.FileInfo{Path: "/", Name: "/", IsDir: true, Size: storage.SizeUnknown}, nil
	}

	var entry *ftpc.Entry
	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		var statErr error
		entry, statErr = conn.GetEntry(s.full(p))
		return statErr
	})
	if err != nil {
		return nil, s.wrapErr("stat", p, err)
	}

	fi := toFileInfo(entry, path.Dir(cp))
	fi.Path, fi.Name = cp, path.Base(cp)
	return &fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	fi, err := s.Stat(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	if fi.IsDir {
		return nil, nil, s.wrapErr("open", p, storage.ErrIsDir)
	}

	rc, err := s.openAt(ctx, p, 0)
	if err != nil {
		return nil, nil, err
	}
	return rc, fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	rc, err := s.openAt(ctx, p, offset)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return rc, nil
	}
	return readCloser{Reader: io.LimitReader(rc, length), Closer: rc}, nil
}

// openAt は指定の位置から読み出します。
//
// 読み終わるまで接続を占有するので、閉じたときに戻します。
func (s *Storage) openAt(ctx context.Context, p string, offset int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, s.wrapErr("open", p, err)
	}

	conn, err := s.pool.get(ctx)
	if err != nil {
		return nil, s.wrapErr("open", p, err)
	}

	res, err := conn.RetrFrom(s.full(p), uint64(offset))
	if err != nil {
		if isConnectionBroken(err) {
			s.pool.discard(conn)
		} else {
			s.pool.put(conn)
		}
		return nil, s.wrapErr("open", p, err)
	}

	return &pooledReader{
		ctx:  ctx,
		res:  res,
		pool: s.pool,
		conn: conn,
	}, nil
}

// pooledReader は読み終わったら接続を戻します。
type pooledReader struct {
	ctx  context.Context
	res  *ftpc.Response
	pool *connPool
	conn *ftpc.ServerConn
	once sync.Once
}

func (r *pooledReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.res.Read(p)
}

func (r *pooledReader) Close() error {
	var err error
	r.once.Do(func() {
		err = r.res.Close()
		if err != nil || r.ctx.Err() != nil {
			// 途中でやめた場合、通信路の状態が分からない。
			r.pool.discard(r.conn)
			return
		}
		r.pool.put(r.conn)
	})
	return err
}

// readCloser は読み手と閉じ手を組み合わせます。
type readCloser struct {
	io.Reader
	io.Closer
}

// Put はファイルを書き込みます。
//
// 別名で書いてから置き換えます。途中で止めても、中身の欠けた
// ファイルが本来の場所に残ることはありません。
func (s *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	cp := cleanPath(p)
	if cp == "/" {
		return nil, s.wrapErr("put", p, errors.New("起点をファイルとして書き込むことはできません"))
	}

	if err := s.ensureDir(ctx, path.Dir(s.full(p))); err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	dst := s.full(p)
	tmp := tempPath(dst)
	counting := &countingReader{r: &ctxReader{ctx: ctx, r: r}}

	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		if err := conn.Stor(tmp, counting); err != nil {
			// 書きかけを残さない。
			_ = conn.Delete(tmp)
			return err
		}

		if !meta.ModTime.IsZero() && s.canSetModTime {
			// 置き換える前に時刻を合わせる。あとから変えると、
			// 失敗したときに時刻だけ違うファイルが残る。
			if err := conn.SetTime(tmp, meta.ModTime); err != nil {
				_ = conn.Delete(tmp)
				return err
			}
		}

		if err := replace(conn, tmp, dst); err != nil {
			_ = conn.Delete(tmp)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	fi := &storage.FileInfo{
		Path: cp,
		Name: path.Base(cp),
		Size: counting.n,
	}
	if s.canSetModTime {
		fi.ModTime = meta.ModTime
	}
	return fi, nil
}

// replace は一時ファイルを本来の場所へ移します。
//
// FTP の改名は置き換え先があると失敗します。先にどけるので、
// ごく短い間ですが置き換え先が存在しない時間ができます。
func replace(conn *ftpc.ServerConn, tmp, dst string) error {
	if err := conn.Rename(tmp, dst); err == nil {
		return nil
	}

	if err := conn.Delete(dst); err != nil && !isNotFound(err) {
		return err
	}
	return conn.Rename(tmp, dst)
}

// tempPath は書き込み中の名前を組み立てます。
func tempPath(dst string) string {
	return path.Join(path.Dir(dst), "."+path.Base(dst)+partSuffix)
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

// Mkdir はディレクトリを（必要なら親ごと）作ります。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	if cleanPath(dir) == "/" {
		return nil
	}

	full := s.full(dir)
	if err := s.mkdirAll(ctx, full); err != nil {
		return s.wrapErr("mkdir", dir, err)
	}
	s.dirs.Remember(full)
	return nil
}

// ensureDir は書き込み先のディレクトリを用意します。
func (s *Storage) ensureDir(ctx context.Context, dir string) error {
	return s.dirs.Ensure(ctx, dir, s.mkdirAll)
}

// mkdirAll は親をさかのぼって作ります。
func (s *Storage) mkdirAll(ctx context.Context, full string) error {
	return s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		return mkdirAllOn(conn, full)
	})
}

func mkdirAllOn(conn *ftpc.ServerConn, full string) error {
	if full == "" || full == "/" || full == "." {
		return nil
	}

	if entry, err := conn.GetEntry(full); err == nil {
		if entry.Type == ftpc.EntryTypeFolder {
			return nil
		}
		return fmt.Errorf("%w: %s は既にファイルです", storage.ErrExist, full)
	}

	if err := mkdirAllOn(conn, path.Dir(full)); err != nil {
		return err
	}

	if err := conn.MakeDir(full); err != nil {
		// 並行して作られた場合に備える。
		if entry, statErr := conn.GetEntry(full); statErr == nil && entry.Type == ftpc.EntryTypeFolder {
			return nil
		}
		return err
	}
	return nil
}

// Remove は1つのファイル、または空のディレクトリを削除します。
func (s *Storage) Remove(ctx context.Context, p string) error {
	cp := cleanPath(p)
	if cp == "/" {
		return s.wrapErr("remove", p, errors.New("起点は削除できません"))
	}

	full := s.full(p)
	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		entry, err := conn.GetEntry(full)
		if err != nil {
			return err
		}

		if entry.Type != ftpc.EntryTypeFolder {
			return conn.Delete(full)
		}

		// FTP の RMD は空でないディレクトリを断るが、
		// 何が起きたのかが分かる失敗にするため先に確かめる。
		children, err := conn.List(full)
		if err != nil {
			return err
		}
		if len(meaningfulEntries(children)) > 0 {
			return fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty)
		}
		return conn.RemoveDir(full)
	})
	if err != nil {
		return s.wrapErr("remove", p, err)
	}

	s.dirs.Forget(full)
	return nil
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if cleanPath(dir) == "/" {
		return s.wrapErr("purge", dir, errors.New("起点は削除できません"))
	}

	full := s.full(dir)
	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		if _, err := conn.GetEntry(full); err != nil {
			return err
		}
		return removeAllOn(ctx, conn, full)
	})
	if err != nil {
		return s.wrapErr("purge", dir, err)
	}

	s.dirs.Forget(full)
	return nil
}

func removeAllOn(ctx context.Context, conn *ftpc.ServerConn, full string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entry, err := conn.GetEntry(full)
	if err != nil {
		return err
	}
	if entry.Type != ftpc.EntryTypeFolder {
		return conn.Delete(full)
	}

	children, err := conn.List(full)
	if err != nil {
		return err
	}
	for _, child := range meaningfulEntries(children) {
		if err := removeAllOn(ctx, conn, path.Join(full, child.Name)); err != nil {
			return err
		}
	}
	return conn.RemoveDir(full)
}

// meaningfulEntries は自分自身と親を取り除きます。
func meaningfulEntries(entries []*ftpc.Entry) []*ftpc.Entry {
	out := make([]*ftpc.Entry, 0, len(entries))
	for _, e := range entries {
		switch e.Name {
		case ".", "..", "":
			continue
		}
		out = append(out, e)
	}
	return out
}

// Move はサーバー上でファイルを移動・改名します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	dst := s.full(dstPath)
	if err := s.ensureDir(ctx, path.Dir(dst)); err != nil {
		return s.wrapErr("move", dstPath, err)
	}

	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		return replace(conn, s.full(srcPath), dst)
	})
	return s.wrapErr("move", srcPath, err)
}

// SetModTime は最終更新時刻を変えます。
func (s *Storage) SetModTime(ctx context.Context, p string, t time.Time) error {
	if !s.canSetModTime {
		return s.wrapErr("setmodtime", p, fmt.Errorf(
			"%w: この相手は更新時刻の書き換えに応じません", storage.ErrUnsupported))
	}

	err := s.withConn(ctx, func(conn *ftpc.ServerConn) error {
		return conn.SetTime(s.full(p), t)
	})
	return s.wrapErr("setmodtime", p, err)
}

// toFileInfo は FTP の一覧の1件を storage.FileInfo にします。
func toFileInfo(e *ftpc.Entry, dir string) storage.FileInfo {
	fi := storage.FileInfo{
		Path:    path.Join(dir, e.Name),
		Name:    e.Name,
		IsDir:   e.Type == ftpc.EntryTypeFolder,
		Size:    int64(e.Size),
		ModTime: e.Time,
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
