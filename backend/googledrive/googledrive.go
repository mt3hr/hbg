// Package googledrive は Google Drive を storage.Storage として実装します。
//
// 以前の実装との主な違いは次の5点です。
//
//   - 一覧の続きをたどります。以前は nextPageToken を要求しながら
//     たどっておらず、子が1000件を超えるフォルダで超過分が
//     エラーも警告もなく欠落していました。
//
//   - パスの解決に失敗したらその場で止まります。以前は途中の段が
//     見つからないと黙って読み飛ばし、別のディレクトリの中身を
//     正しい結果として返していました。
//
//   - すべての呼び出しに context を渡します。
//
//   - ハッシュを取得します。転送内容の検証に使えます。
//
//   - 削除は既定でゴミ箱に入れます。以前は完全削除でした。
package googledrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/mt3hr/hbg/storage"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

// Type はこのバックエンドの種別名です。
const Type = "googledrive"

// folderMIME はフォルダを表す MIME 型です。
const folderMIME = "application/vnd.google-apps.folder"

// nativePrefix は Google ドキュメントなどネイティブ形式の MIME 型の接頭辞です。
const nativePrefix = "application/vnd.google-apps."

// fileFields は1件について取得する項目です。
//
// ハッシュとサイズを最初から要求しておくことで、
// 転送の判断のために取り直す必要がなくなります。
const fileFields = "id,name,mimeType,modifiedTime,size,md5Checksum,sha1Checksum,sha256Checksum,trashed,parents"

// listFields は一覧で取得する項目です。
const listFields = "nextPageToken,files(" + fileFields + ")"

// listPageSize は一覧が1回に要求する件数です。
const listPageSize = 1000

// uploadChunkSize は分割送信の1回ぶんの大きさです。
// これより小さいものは1回の要求で送ります。
var uploadChunkSize = googleapi.DefaultUploadChunkSize

// ネイティブ形式の扱い方。
const (
	// nativeError はネイティブ形式を読もうとしたら失敗させます。
	nativeError = "error"
	// nativeSkip はネイティブ形式を一覧に出しません。
	nativeSkip = "skip"
)

// Storage は Google Drive です。
type Storage struct {
	srv  *drive.Service
	name string

	// rootID はこのストレージのルートにあたるフォルダのIDです。
	rootID string
	// driveID は共有ドライブのIDです。空ならマイドライブです。
	driveID string
	// nativeFiles はネイティブ形式の扱いです。
	nativeFiles string
	// useTrash が真なら、削除はゴミ箱に入れます。
	useTrash bool

	resolver *resolver
}

// New は保存済みのトークンを使って Google Drive に接続します。
//
// トークンがない場合はエラーを返すので、hbg auth login <名前> で
// 認証してください。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	srv, err := newService(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("googledrive %s を開けませんでした: %w", cfg.Name, err)
	}
	return newWithService(cfg, srv)
}

// newWithService は用意済みのクライアントからストレージを作ります。
// 偽のサーバーに向けた試験でも使います。
func newWithService(cfg Config, srv *drive.Service) (*Storage, error) {
	nativeFiles := cfg.NativeFiles
	if nativeFiles == "" {
		nativeFiles = nativeError
	}
	if nativeFiles != nativeError && nativeFiles != nativeSkip {
		return nil, fmt.Errorf("native_files には %q か %q を指定してください（%q が指定されました）",
			nativeError, nativeSkip, cfg.NativeFiles)
	}

	rootID := cfg.RootFolderID
	if rootID == "" {
		rootID = cfg.DriveID
	}
	if rootID == "" {
		rootID = "root"
	}

	useTrash := true
	if cfg.UseTrash != nil {
		useTrash = *cfg.UseTrash
	}

	s := &Storage{
		srv:         srv,
		name:        cfg.Name,
		rootID:      rootID,
		driveID:     cfg.DriveID,
		nativeFiles: nativeFiles,
		useTrash:    useTrash,
	}
	s.resolver = newResolver(s)
	return s, nil
}

// Type はストレージの種別を返します。
func (g *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (g *Storage) Name() string { return g.name }

// Features は Google Drive にできることを返します。
func (g *Storage) Features() *storage.Features {
	return &storage.Features{
		// RFC3339 で表され、実質ミリ秒まで保たれます。
		ModTimePrecision: time.Millisecond,
		CanSetModTime:    true,
		// 同じフォルダに同名のファイルを作れてしまう点に注意。
		CaseInsensitive: false,
		Hashes:          storage.HashSet{storage.SHA256, storage.SHA1, storage.MD5},
		ImplicitDirs:    true,
		EmptyDirs:       true,
		AtomicPut:       true,
	}
}

// Close はストレージを閉じます。
func (g *Storage) Close() error {
	g.srv = nil
	return nil
}

// List はフォルダの直下を1件ずつ fn に渡します。
//
// 続きは nextPageToken をたどって取得します。
// 順序は API が返した順のままです。以前は map に溜めていたため、
// 実行のたびに順序が変わっていました。
func (g *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	dirID, err := g.resolver.dirID(ctx, dir)
	if err != nil {
		return g.wrapErr("list", dir, err)
	}

	base := cleanPath(dir)
	var cbErr error

	call := g.listCall(ctx, fmt.Sprintf("'%s' in parents and trashed = false", escapeQuery(dirID)))
	err = call.Pages(ctx, func(page *drive.FileList) error {
		for _, f := range page.Files {
			if g.skipNative(f) {
				continue
			}
			if cbErr = fn(g.toFileInfo(f, base)); cbErr != nil {
				// Pages を止めるためにエラーを返す。
				// 呼び出し側の意図した値をそのまま返せるよう控えておく。
				return cbErr
			}
		}
		return nil
	})

	switch {
	case cbErr != nil:
		return cbErr
	case err != nil:
		return g.wrapErr("list", dir, err)
	}
	return nil
}

// listCall は一覧の呼び出しを組み立てます。
func (g *Storage) listCall(ctx context.Context, q string) *drive.FilesListCall {
	call := g.srv.Files.List().
		Context(ctx).
		Q(q).
		PageSize(listPageSize).
		Fields(listFields).
		SupportsAllDrives(true).
		IncludeItemsFromAllDrives(true)

	if g.driveID != "" {
		call = call.Corpora("drive").DriveId(g.driveID)
	}
	return call
}

// findChild は親フォルダの中から名前で1件を探します。
// 見つからない場合は nil を返します（エラーではありません）。
//
// 以前はフォルダの中身を全件取得してから突き合わせていました。
// 名前で絞り込めば、件数によらず1回のやりとりで済みます。
func (g *Storage) findChild(ctx context.Context, parentID, name string) (*drive.File, error) {
	q := fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false",
		escapeQuery(name), escapeQuery(parentID))

	res, err := g.listCall(ctx, q).PageSize(10).Do()
	if err != nil {
		return nil, err
	}
	if len(res.Files) == 0 {
		return nil, nil
	}

	// Drive は同じフォルダに同名のものを複数作れる。
	// どれを指しているか一意に決まらないので、更新の新しいものを選ぶ。
	// 選び方が実行ごとに変わらないよう、時刻が同じ場合はIDで決める。
	best := res.Files[0]
	for _, f := range res.Files[1:] {
		if newerThan(f, best) {
			best = f
		}
	}
	return best, nil
}

// newerThan は a のほうが新しいかを返します。
func newerThan(a, b *drive.File) bool {
	if a.ModifiedTime != b.ModifiedTime {
		return a.ModifiedTime > b.ModifiedTime
	}
	return a.Id > b.Id
}

// Stat は1件のメタデータを返します。
func (g *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	file, err := g.resolver.file(ctx, p)
	if err != nil {
		return nil, g.wrapErr("stat", p, err)
	}

	fi := g.toFileInfo(file, path.Dir(cleanPath(p)))
	if cleanPath(p) == "/" {
		fi.Path, fi.Name = "/", "/"
	}
	return &fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (g *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	file, err := g.resolver.file(ctx, p)
	if err != nil {
		return nil, nil, g.wrapErr("open", p, err)
	}
	if err = g.checkDownloadable(file); err != nil {
		return nil, nil, g.wrapErr("open", p, err)
	}

	res, err := g.srv.Files.Get(file.Id).
		Context(ctx).
		SupportsAllDrives(true).
		Download()
	if err != nil {
		return nil, nil, g.wrapErr("open", p, err)
	}

	fi := g.toFileInfo(file, path.Dir(cleanPath(p)))
	return res.Body, &fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
func (g *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	file, err := g.resolver.file(ctx, p)
	if err != nil {
		return nil, g.wrapErr("open", p, err)
	}
	if err = g.checkDownloadable(file); err != nil {
		return nil, g.wrapErr("open", p, err)
	}

	call := g.srv.Files.Get(file.Id).Context(ctx).SupportsAllDrives(true)
	call.Header().Set("Range", rangeHeader(offset, length))

	res, err := call.Download()
	if err != nil {
		return nil, g.wrapErr("open", p, err)
	}
	return res.Body, nil
}

func rangeHeader(offset, length int64) string {
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// checkDownloadable は、そのまま読み出せるファイルかを確かめます。
func (g *Storage) checkDownloadable(file *drive.File) error {
	switch {
	case file.MimeType == folderMIME:
		return storage.ErrIsDir
	case isNative(file.MimeType):
		// Google ドキュメントなどは実体を持たず、書き出し形式を
		// 選んで変換しないと取り出せない。以前の実装はサイズが 0 と
		// 伝わることに気づかず、中身が空のファイルを作っていた。
		return fmt.Errorf("%w: %s は Google の独自形式（%s）なので、そのままでは取り出せません。"+
			"設定で native_files: skip を指定すると一覧から外せます",
			storage.ErrUnsupported, file.Name, file.MimeType)
	}
	return nil
}

// Put はファイルを書き込みます。
func (g *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	cp := cleanPath(p)
	if cp == "/" {
		return nil, g.wrapErr("put", p, errors.New("ルートをファイルとして書き込むことはできません"))
	}

	dir, name := path.Dir(cp), path.Base(cp)
	parentID, err := g.resolver.dirIDCreating(ctx, dir)
	if err != nil {
		return nil, g.wrapErr("put", p, err)
	}

	existing, err := g.findChild(ctx, parentID, name)
	if err != nil {
		return nil, g.wrapErr("put", p, err)
	}

	file := &drive.File{Name: name}
	if !meta.ModTime.IsZero() {
		file.ModifiedTime = meta.ModTime.UTC().Format(time.RFC3339Nano)
	}

	opts := []googleapi.MediaOption{googleapi.ChunkSize(uploadChunkSize)}
	if meta.MIMEType != "" {
		opts = append(opts, googleapi.ContentType(meta.MIMEType))
	}

	var written *drive.File
	if existing != nil {
		// 更新のときに親を指定すると弾かれる。
		written, err = g.srv.Files.Update(existing.Id, file).
			Context(ctx).
			SupportsAllDrives(true).
			Fields(fileFields).
			Media(r, opts...).
			Do()
	} else {
		file.Parents = []string{parentID}
		written, err = g.srv.Files.Create(file).
			Context(ctx).
			SupportsAllDrives(true).
			Fields(fileFields).
			Media(r, opts...).
			Do()
	}
	if err != nil {
		return nil, g.wrapErr("put", p, err)
	}

	fi := g.toFileInfo(written, dir)
	return &fi, nil
}

// Mkdir はフォルダを（必要なら親ごと）作ります。すでにあれば何もしません。
func (g *Storage) Mkdir(ctx context.Context, dir string) error {
	if _, err := g.resolver.dirIDCreating(ctx, dir); err != nil {
		return g.wrapErr("mkdir", dir, err)
	}
	return nil
}

// createFolder はフォルダを1つ作ります。
func (g *Storage) createFolder(ctx context.Context, parentID, name string) (*drive.File, error) {
	return g.srv.Files.Create(&drive.File{
		Name:     name,
		MimeType: folderMIME,
		Parents:  []string{parentID},
	}).
		Context(ctx).
		SupportsAllDrives(true).
		Fields(fileFields).
		Do()
}

// Remove は1つのファイル、または空のフォルダを削除します。
//
// 既定ではゴミ箱に入れます。以前は完全削除だったため、
// 誤って消したものを取り戻す手立てがありませんでした。
func (g *Storage) Remove(ctx context.Context, p string) error {
	cp := cleanPath(p)
	if cp == "/" {
		return g.wrapErr("remove", p, errors.New("ルートは削除できません"))
	}

	file, err := g.resolver.file(ctx, cp)
	if err != nil {
		return g.wrapErr("remove", p, err)
	}

	if file.MimeType == folderMIME {
		empty, err := g.isEmpty(ctx, file.Id)
		if err != nil {
			return g.wrapErr("remove", p, err)
		}
		if !empty {
			return g.wrapErr("remove", p,
				fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty))
		}
	}

	return g.wrapErr("remove", p, g.discard(ctx, cp, file.Id))
}

// Purge はフォルダを中身ごと削除します。
func (g *Storage) Purge(ctx context.Context, dir string) error {
	cp := cleanPath(dir)
	if cp == "/" {
		return g.wrapErr("purge", dir, errors.New("ルートは削除できません"))
	}

	file, err := g.resolver.file(ctx, cp)
	if err != nil {
		return g.wrapErr("purge", dir, err)
	}
	return g.wrapErr("purge", dir, g.discard(ctx, cp, file.Id))
}

// discard は1件を捨てます。
func (g *Storage) discard(ctx context.Context, p, id string) error {
	// 覚えていたIDを捨てる。消したあとに同じ名前で作り直された場合、
	// 古いIDを掴んだままだと存在しないものを操作することになる。
	g.resolver.forget(p)

	if !g.useTrash {
		return g.srv.Files.Delete(id).Context(ctx).SupportsAllDrives(true).Do()
	}
	_, err := g.srv.Files.Update(id, &drive.File{Trashed: true}).
		Context(ctx).
		SupportsAllDrives(true).
		Fields("id").
		Do()
	return err
}

// isEmpty はフォルダが空かを返します。
func (g *Storage) isEmpty(ctx context.Context, id string) (bool, error) {
	res, err := g.listCall(ctx, fmt.Sprintf("'%s' in parents and trashed = false", escapeQuery(id))).
		PageSize(1).
		Do()
	if err != nil {
		return false, err
	}
	return len(res.Files) == 0, nil
}

// Hash はファイルのハッシュを返します。
func (g *Storage) Hash(ctx context.Context, p string, ht storage.HashType) (string, error) {
	file, err := g.resolver.file(ctx, p)
	if err != nil {
		return "", g.wrapErr("hash", p, err)
	}

	h := hashOf(file, ht)
	if h == "" {
		return "", g.wrapErr("hash", p, fmt.Errorf(
			"%w: %s のハッシュ %s を取得できません", storage.ErrUnsupported, file.Name, ht))
	}
	return h, nil
}

// hashOf はメタデータからハッシュを取り出します。
func hashOf(file *drive.File, ht storage.HashType) string {
	switch ht {
	case storage.MD5:
		return file.Md5Checksum
	case storage.SHA1:
		return file.Sha1Checksum
	case storage.SHA256:
		return file.Sha256Checksum
	}
	return ""
}

// ServerSideCopy は内容を転送せずにコピーします。
func (g *Storage) ServerSideCopy(ctx context.Context, srcPath, dstPath string) (*storage.FileInfo, error) {
	src, err := g.resolver.file(ctx, srcPath)
	if err != nil {
		return nil, g.wrapErr("copy", srcPath, err)
	}

	dir, name := path.Dir(cleanPath(dstPath)), path.Base(cleanPath(dstPath))
	parentID, err := g.resolver.dirIDCreating(ctx, dir)
	if err != nil {
		return nil, g.wrapErr("copy", dstPath, err)
	}

	// すでにあるものはどける。Drive は同名を許すので、
	// そのままコピーすると同じ名前のものが2つできてしまう。
	if err = g.removeIfExists(ctx, parentID, dir, name); err != nil {
		return nil, g.wrapErr("copy", dstPath, err)
	}

	copied, err := g.srv.Files.Copy(src.Id, &drive.File{
		Name:    name,
		Parents: []string{parentID},
	}).
		Context(ctx).
		SupportsAllDrives(true).
		Fields(fileFields).
		Do()
	if err != nil {
		return nil, g.wrapErr("copy", srcPath, err)
	}

	fi := g.toFileInfo(copied, dir)
	return &fi, nil
}

// Move は内容を転送せずに移動・改名します。
func (g *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	src, err := g.resolver.file(ctx, srcPath)
	if err != nil {
		return g.wrapErr("move", srcPath, err)
	}
	srcParentID, err := g.resolver.dirID(ctx, path.Dir(cleanPath(srcPath)))
	if err != nil {
		return g.wrapErr("move", srcPath, err)
	}

	dir, name := path.Dir(cleanPath(dstPath)), path.Base(cleanPath(dstPath))
	dstParentID, err := g.resolver.dirIDCreating(ctx, dir)
	if err != nil {
		return g.wrapErr("move", dstPath, err)
	}

	if err = g.removeIfExists(ctx, dstParentID, dir, name); err != nil {
		return g.wrapErr("move", dstPath, err)
	}

	g.resolver.forget(srcPath)

	call := g.srv.Files.Update(src.Id, &drive.File{Name: name}).
		Context(ctx).
		SupportsAllDrives(true).
		Fields("id")
	if srcParentID != dstParentID {
		call = call.AddParents(dstParentID).RemoveParents(srcParentID)
	}

	_, err = call.Do()
	return g.wrapErr("move", srcPath, err)
}

// removeIfExists は、その場所にすでにあるものを捨てます。
func (g *Storage) removeIfExists(ctx context.Context, parentID, dir, name string) error {
	existing, err := g.findChild(ctx, parentID, name)
	if err != nil || existing == nil {
		return err
	}
	return g.discard(ctx, path.Join(dir, name), existing.Id)
}

// --- メタデータの変換 ---

// skipNative は、その1件を一覧から外すかを返します。
func (g *Storage) skipNative(file *drive.File) bool {
	return g.nativeFiles == nativeSkip && isNative(file.MimeType)
}

// isNative は Google の独自形式かを返します。フォルダは含みません。
func isNative(mimeType string) bool {
	return strings.HasPrefix(mimeType, nativePrefix) && mimeType != folderMIME
}

// toFileInfo は Drive のメタデータを storage.FileInfo にします。
func (g *Storage) toFileInfo(file *drive.File, dir string) storage.FileInfo {
	fi := storage.FileInfo{
		Path:  path.Join(dir, file.Name),
		Name:  file.Name,
		IsDir: file.MimeType == folderMIME,
		Size:  file.Size,
		ID:    file.Id,
	}

	if fi.IsDir || isNative(file.MimeType) {
		// フォルダにも独自形式にも「バイト数」がない。
		// 0 と申告すると、空のファイルと区別がつかなくなる。
		fi.Size = storage.SizeUnknown
	}

	if file.ModifiedTime != "" {
		// 解釈できない時刻は「不明」として扱う。
		// 以前はここで失敗して一覧全体が取れなくなっていた。
		if t, err := time.Parse(time.RFC3339, file.ModifiedTime); err == nil {
			fi.ModTime = t
		}
	}

	hashes := map[storage.HashType]string{}
	for _, ht := range []storage.HashType{storage.MD5, storage.SHA1, storage.SHA256} {
		if h := hashOf(file, ht); h != "" {
			hashes[ht] = h
		}
	}
	if len(hashes) > 0 {
		fi.Hashes = hashes
	}
	return fi
}

var (
	_ storage.Storage          = (*Storage)(nil)
	_ storage.Hasher           = (*Storage)(nil)
	_ storage.Purger           = (*Storage)(nil)
	_ storage.Mover            = (*Storage)(nil)
	_ storage.RangeOpener      = (*Storage)(nil)
	_ storage.ServerSideCopier = (*Storage)(nil)
)
