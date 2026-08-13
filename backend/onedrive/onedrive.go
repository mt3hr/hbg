// Package onedrive は OneDrive を storage.Storage として実装します。
//
// 個人用の OneDrive のほか、職場・学校のアカウントや
// SharePoint のドキュメントライブラリにも繋げます。
package onedrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "onedrive"

// smallUploadLimit は、1回の要求で送りきる大きさの上限です。
//
// Graph は 4MiB までを1回で受け付けます。それを超えるものは
// 分割送信に切り替えます。
var smallUploadLimit int64 = 4 * 1024 * 1024

// chunkUnit は分割送信の1つぶんの大きさの単位です。
//
// Graph は「320KiB の倍数」であることを求めます。
// 端数があると受け付けられません。
const chunkUnit = 320 * 1024

// defaultChunkSize は分割送信の1つぶんの既定の大きさです。
// 公式には5〜10MiB が勧められています。
var defaultChunkSize int64 = 10 * chunkUnit // 3200KiB

// Storage は OneDrive です。
type Storage struct {
	name   string
	client *graphClient
	root   string

	chunkSize int64

	// knownDirs は「ある」と分かっているディレクトリです。
	//
	// Graph の書き込みは、親がないと 404 で断られます。かといって
	// 書き込みのたびに確かめると要求が倍になります。一度作った
	// （あるいはあると分かった）ものを覚えておけば、どちらも避けられます。
	dirsMu    sync.Mutex
	knownDirs map[string]struct{}
}

// New は OneDrive に接続します。
//
// ここでは通信しません。繋がるかどうかは最初の操作で分かります。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("onedrive %s: %w", cfg.Name, err)
	}

	httpClient, err := newHTTPClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("onedrive %s: %w", cfg.Name, err)
	}

	base := graphBase
	if cfg.baseOverride != "" {
		base = cfg.baseOverride
	}

	return &Storage{
		name: cfg.Name,
		client: &graphClient{
			http:      httpClient,
			base:      base,
			driveRoot: cfg.driveRoot(),
		},
		root:      strings.Trim(cleanPath(cfg.Root), "/"),
		chunkSize: defaultChunkSize,
		knownDirs: map[string]struct{}{},
	}, nil
}

// Type はストレージの種別を返します。
func (s *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (s *Storage) Name() string { return s.name }

// Features は OneDrive にできることを返します。
func (s *Storage) Features() *storage.Features {
	return &storage.Features{
		// RFC3339 で表され、実質ミリ秒まで保たれます。
		ModTimePrecision: time.Millisecond,
		CanSetModTime:    true,
		CaseInsensitive:  true,
		// OneDrive が返すのは quickXorHash で、hbg はこれを
		// 計算できません。詳しくは README の既知の制限を見てください。
		Hashes:       nil,
		ImplicitDirs: true,
		EmptyDirs:    true,
		// 書き込みは完了してはじめて見えます。
		AtomicPut: true,
		// OneDrive のファイル名に使えない文字。
		IllegalChars: `<>:"|?*\`,
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
		return p
	}
	if p == "" {
		return s.root
	}
	return s.root + "/" + p
}

// cleanPath はパスを正規化します。
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
func (s *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	base := cleanPath(dir)

	var cbErr error
	err := s.client.listChildren(ctx, s.full(dir), func(item driveItem) error {
		cbErr = fn(toFileInfo(item, base))
		return cbErr
	})

	switch {
	case cbErr != nil:
		return cbErr
	case err != nil:
		return s.wrapErr("list", dir, err)
	}
	return nil
}

// Stat は1件のメタデータを返します。
func (s *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	item, err := s.client.getItem(ctx, s.full(p))
	if err != nil {
		return nil, s.wrapErr("stat", p, err)
	}

	cp := cleanPath(p)
	fi := toFileInfo(*item, path.Dir(cp))
	fi.Path = cp
	if cp == "/" {
		fi.Name, fi.IsDir = "/", true
	}
	return &fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (s *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	item, err := s.client.getItem(ctx, s.full(p))
	if err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}
	if item.isDir() {
		return nil, nil, s.wrapErr("open", p, storage.ErrIsDir)
	}

	rc, err := s.client.getContent(ctx, s.full(p), nil)
	if err != nil {
		return nil, nil, s.wrapErr("open", p, err)
	}

	cp := cleanPath(p)
	fi := toFileInfo(*item, path.Dir(cp))
	fi.Path = cp
	return rc, &fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (s *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	rc, err := s.client.getContent(ctx, s.full(p), map[string]string{
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
// meta.Size は信用しません。実際に読み終わるまでを書き込みます。
func (s *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	cp := cleanPath(p)
	if cp == "/" {
		return nil, s.wrapErr("put", p, errors.New("起点をファイルとして書き込むことはできません"))
	}

	if err := s.ensureDir(ctx, path.Dir(s.full(p))); err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	// 先を読んでみて、収まりきるなら1回の要求で送る。
	head, atEOF, err := readHead(r, smallUploadLimit)
	if err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	var item *driveItem
	if atEOF {
		item, err = s.client.uploadSmall(ctx, s.full(p), head, meta.ModTime)
	} else {
		item, err = s.uploadLarge(ctx, s.full(p), head, r, meta.ModTime)
	}
	if err != nil {
		return nil, s.wrapErr("put", p, err)
	}

	fi := toFileInfo(*item, path.Dir(cp))
	fi.Path = cp
	return &fi, nil
}

// ensureDir は書き込み先のディレクトリを用意します。
//
// 一度確かめたものは覚えておくので、同じディレクトリへ続けて
// 書き込むあいだは要求が増えません。転送の側が先に Mkdir を
// 呼んでいれば、そこで覚えたぶんが効いて1度も増えません。
func (s *Storage) ensureDir(ctx context.Context, dir string) error {
	if dir == "" || dir == "." || dir == "/" || s.dirIsKnown(dir) {
		return nil
	}

	// 同じディレクトリへ並行して書き込むとき、作成がぶつからない
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

// readHead は先頭を最大 limit バイト読み、読みきったかどうかを返します。
//
// limit ぴったりのファイルを「まだ続きがある」と誤らないよう、
// 1バイト余分に読んで判断します。
func readHead(r io.Reader, limit int64) (buf []byte, atEOF bool, err error) {
	buf = make([]byte, limit+1)
	n, err := io.ReadFull(r, buf)
	switch {
	case err == nil:
		return buf, false, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return buf[:n], true, nil
	default:
		return nil, false, err
	}
}

// uploadLarge は分割して送ります。
//
// Graph の分割送信は順番どおりに送る必要があり、1つぶんの大きさは
// 320KiB の倍数でなければなりません（最後のひとかたまりを除く）。
// 全体の大きさは最後のひとかたまりを送る時点で分かればよいので、
// 事前にサイズが分からなくても送れます。
func (s *Storage) uploadLarge(ctx context.Context, full string, head []byte, rest io.Reader, modTime time.Time) (*driveItem, error) {
	uploadURL, err := s.client.createUploadSession(ctx, full, modTime)
	if err != nil {
		return nil, err
	}

	r := io.MultiReader(newBytesReader(head), rest)
	offset := int64(0)

	// 次のひとかたまりを先に読んでおく。読めた量が足りなければ
	// そこが最後なので、全体の大きさが決まる。
	buf := make([]byte, s.chunkSize)
	pending, atEOF, err := readChunk(r, buf)
	if err != nil {
		s.client.cancelUploadSession(ctx, uploadURL)
		return nil, err
	}

	for {
		total := int64(-1)
		if atEOF {
			total = offset + int64(len(pending))
		}

		if total >= 0 {
			item, err := s.client.uploadChunk(ctx, uploadURL, pending, offset, total)
			if err != nil {
				s.client.cancelUploadSession(ctx, uploadURL)
				return nil, err
			}
			if item == nil {
				s.client.cancelUploadSession(ctx, uploadURL)
				return nil, fmt.Errorf("分割送信を終えたのに結果が返ってきませんでした")
			}
			return item, nil
		}

		// まだ続きがある。全体の大きさは分からないので、
		// 送るのは確定したぶんだけにして次を読む。
		next := make([]byte, s.chunkSize)
		nextChunk, nextEOF, err := readChunk(r, next)
		if err != nil {
			s.client.cancelUploadSession(ctx, uploadURL)
			return nil, err
		}

		// 続きがあると分かったので、いま持っているぶんを送る。
		// 全体の大きさが未定のあいだは "*" で伝える。
		if _, err := s.client.uploadChunk(ctx, uploadURL, pending, offset, unknownTotal); err != nil {
			s.client.cancelUploadSession(ctx, uploadURL)
			return nil, err
		}
		offset += int64(len(pending))
		pending, atEOF = nextChunk, nextEOF
	}
}

// readChunk はひとかたまり読みます。読みきったかどうかも返します。
func readChunk(r io.Reader, buf []byte) ([]byte, bool, error) {
	n, err := io.ReadFull(r, buf)
	switch {
	case err == nil:
		return buf[:n], false, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return buf[:n], true, nil
	default:
		return nil, false, err
	}
}

// Mkdir はディレクトリを（必要なら親ごと）作ります。
func (s *Storage) Mkdir(ctx context.Context, dir string) error {
	cp := cleanPath(dir)
	if cp == "/" {
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
	if full == "" || full == "." || full == "/" {
		return nil
	}

	item, err := s.client.getItem(ctx, full)
	switch {
	case err == nil && item.isDir():
		return nil
	case err == nil:
		return fmt.Errorf("%w: %s は既にファイルです", storage.ErrExist, full)
	case !isNotFound(err):
		return err
	}

	parent := path.Dir(full)
	if parent == "." {
		parent = ""
	}
	if err := s.mkdirAll(ctx, parent); err != nil {
		return err
	}

	if _, err := s.client.createFolder(ctx, parent, path.Base(full)); err != nil {
		// 並行して作られた場合に備える。
		if item, statErr := s.client.getItem(ctx, full); statErr == nil && item.isDir() {
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

	item, err := s.client.getItem(ctx, s.full(p))
	if err != nil {
		return s.wrapErr("remove", p, err)
	}

	// OneDrive の削除はフォルダを中身ごと消す。
	// Remove の約束を守るため、空であることを先に確かめる。
	if item.isDir() && item.Folder.ChildCount > 0 {
		return s.wrapErr("remove", p,
			fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty))
	}

	s.forgetDir(s.full(p))
	return s.wrapErr("remove", p, s.client.deleteItem(ctx, s.full(p)))
}

// Purge はディレクトリを中身ごと削除します。
func (s *Storage) Purge(ctx context.Context, dir string) error {
	if cleanPath(dir) == "/" {
		return s.wrapErr("purge", dir, errors.New("起点は削除できません"))
	}

	s.forgetDir(s.full(dir))
	return s.wrapErr("purge", dir, s.client.deleteItem(ctx, s.full(dir)))
}

// Move はサーバー側でファイルを移動・改名します。
func (s *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	dstDir := path.Dir(cleanPath(dstPath))
	if err := s.mkdirAll(ctx, s.full(dstDir)); err != nil {
		return s.wrapErr("move", dstPath, err)
	}

	// 移動先にすでにあるものはどける。
	// OneDrive の移動は上書きを受け付けない。
	if _, err := s.client.getItem(ctx, s.full(dstPath)); err == nil {
		if err := s.client.deleteItem(ctx, s.full(dstPath)); err != nil {
			return s.wrapErr("move", dstPath, err)
		}
	}

	body := map[string]any{
		"name":            path.Base(cleanPath(dstPath)),
		"parentReference": map[string]any{"path": s.parentReference(dstDir)},
	}
	_, err := s.client.patchItem(ctx, s.full(srcPath), body)
	return s.wrapErr("move", srcPath, err)
}

// SetModTime は元のファイルの更新時刻を書き換えます。
func (s *Storage) SetModTime(ctx context.Context, p string, t time.Time) error {
	_, err := s.client.setModTime(ctx, s.full(p), t)
	return s.wrapErr("setmodtime", p, err)
}

// parentReference は移動先の親を表す文字列を組み立てます。
func (s *Storage) parentReference(dir string) string {
	full := s.full(dir)
	if full == "" {
		return "/drive/root:"
	}
	return "/drive/root:/" + full
}

// toFileInfo は Graph の項目を storage.FileInfo にします。
func toFileInfo(item driveItem, dir string) storage.FileInfo {
	fi := storage.FileInfo{
		Path:    path.Join(dir, item.Name),
		Name:    item.Name,
		IsDir:   item.isDir(),
		Size:    item.Size,
		ModTime: item.modTime(),
		ID:      item.ID,
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
