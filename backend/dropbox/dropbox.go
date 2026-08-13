// Package dropbox は Dropbox を storage.Storage として実装します。
//
// 以前の実装との主な違いは次の4点です。
//
//   - すべての呼び出しに context を渡します。Ctrl-C が通信の途中でも効きます。
//
//   - 書き込みが宣言されたサイズに左右されません。以前は全経路が
//     150MiB の io.LimitReader を通っており、サイズが過小に伝わると
//     内容が無警告で切り詰められていました。
//
//   - content hash を取得します。転送内容の検証に使えます。
//
//   - 同一アカウント内のコピーと移動をサーバー側で行います。
package dropbox

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	dbxapi "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/filetransfer"
	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "dropbox"

// maxFileSize は Dropbox が受け付ける1ファイルの上限です。
const maxFileSize int64 = 350 * 1024 * 1024 * 1024

// smallUploadLimit は、1回の要求で送りきる大きさの上限です。
//
// これを超えるものはアップロードセッションに切り替えます。
// 小さいファイルを毎回セッションで送ると、1ファイルにつき
// 要求が2回になり、書き込みの多さによる制限に掛かりやすくなります。
//
// 先頭をここまで読み込んでから判断するため、同時処理数のぶんだけ
// メモリを使います（-w 4 なら最大 32MiB）。
var smallUploadLimit int64 = 8 * 1024 * 1024

// listPageSize は List が一度に要求する件数です。
const listPageSize = 1000

// uploadMaxAttempts はアップロードセッションの1チャンクあたりの試行回数です。
const uploadMaxAttempts = 5

// Storage は Dropbox です。
type Storage struct {
	client   dbx.ContextClient
	uploader *filetransfer.Uploader
	name     string
}

// New は保存済みのトークンを使って Dropbox に接続します。
//
// トークンがない場合はエラーを返すので、hbg auth login <名前> で
// 認証してください。AccessToken が設定されている場合はそれを直接使います。
func New(ctx context.Context, cfg Config) (*Storage, error) {
	client, err := newClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("dropbox %s を開けませんでした: %w", cfg.Name, err)
	}
	return newWithClient(cfg.Name, client), nil
}

// newWithClient は用意済みのクライアントからストレージを作ります。
// 偽のサーバーに向けた試験でも使います。
func newWithClient(name string, client dbx.ContextClient) *Storage {
	return &Storage{
		client:   client,
		uploader: filetransfer.NewUploader(client),
		name:     name,
	}
}

// Type はストレージの種別を返します。
func (d *Storage) Type() string { return Type }

// Name は設定ファイルで付けた名前を返します。
func (d *Storage) Name() string { return d.name }

// Features は Dropbox にできることを返します。
func (d *Storage) Features() *storage.Features {
	return &storage.Features{
		// Dropbox は更新時刻を UTC の秒に丸める。
		ModTimePrecision: time.Second,
		// 書き込み時に指定できる。あとから変更する手段はない。
		CanSetModTime:   true,
		CaseInsensitive: true,
		Hashes:          storage.HashSet{storage.DropboxContent},
		ImplicitDirs:    true,
		EmptyDirs:       true,
		// アップロードは完了時にはじめて見えるので不可分。
		AtomicPut:   true,
		MaxFileSize: maxFileSize,
	}
}

// Close はストレージを閉じます。
func (d *Storage) Close() error {
	d.client = nil
	d.uploader = nil
	return nil
}

// List はディレクトリの直下を1件ずつ fn に渡します。
//
// 続きは cursor をたどって取得します。全件をメモリに溜めません。
func (d *Storage) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	nd := normalize(dir)

	arg := dbx.NewListFolderArg(nd)
	arg.Limit = listPageSize

	res, err := d.client.ListFolderContext(ctx, arg)
	if err != nil {
		return d.wrapErr("list", dir, err)
	}

	for {
		for _, m := range res.Entries {
			fi, ok := toFileInfo(m, nd)
			if !ok {
				// 削除済みの記録などファイルでもディレクトリでもないもの。
				continue
			}
			if err := fn(fi); err != nil {
				return err
			}
		}
		if !res.HasMore {
			return nil
		}

		res, err = d.client.ListFolderContinueContext(ctx, dbx.NewListFolderContinueArg(res.Cursor))
		if err != nil {
			return d.wrapErr("list", dir, err)
		}
	}
}

// Stat は1件のメタデータを返します。
func (d *Storage) Stat(ctx context.Context, p string) (*storage.FileInfo, error) {
	if isRoot(p) {
		return &storage.FileInfo{
			Path:  "/",
			Name:  "/",
			IsDir: true,
			Size:  storage.SizeUnknown,
		}, nil
	}

	np := normalize(p)
	md, err := d.client.GetMetadataContext(ctx, dbx.NewGetMetadataArg(np))
	if err != nil {
		return nil, d.wrapErr("stat", p, err)
	}

	fi, ok := toFileInfo(md, parentOf(np))
	if !ok {
		return nil, d.wrapErr("stat", p, fmt.Errorf("%w (削除済みです)", storage.ErrNotFound))
	}
	return &fi, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (d *Storage) Open(ctx context.Context, p string) (io.ReadCloser, *storage.FileInfo, error) {
	if isRoot(p) {
		return nil, nil, d.wrapErr("open", p, storage.ErrIsDir)
	}

	np := normalize(p)
	md, body, err := d.client.DownloadContext(ctx, dbx.NewDownloadArg(np))
	if err != nil {
		return nil, nil, d.wrapErr("open", p, err)
	}

	fi := fileMetadataToFileInfo(md, parentOf(np))
	return body, &fi, nil
}

// OpenRange は offset から length バイトを読む ReadCloser を返します。
// length が負なら最後までを読みます。
func (d *Storage) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, d.wrapErr("open", p, fmt.Errorf("読み出し位置が負です: %d", offset))
	}

	arg := dbx.NewDownloadArg(normalize(p))
	arg.ExtraHeaders = map[string]string{"Range": rangeHeader(offset, length)}

	_, body, err := d.client.DownloadContext(ctx, arg)
	if err != nil {
		return nil, d.wrapErr("open", p, err)
	}
	return body, nil
}

// rangeHeader は Range ヘッダの値を組み立てます。
func rangeHeader(offset, length int64) string {
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// Put はファイルを書き込みます。
//
// meta.Size は信用しません。実際に読み終わるまでを書き込みます。
// 以前は宣言されたサイズをもとに io.LimitReader を掛けていたため、
// サイズが過小に伝わると内容が無警告で切り詰められていました。
func (d *Storage) Put(ctx context.Context, p string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	if isRoot(p) {
		return nil, d.wrapErr("put", p, errors.New("ルートをファイルとして書き込むことはできません"))
	}
	np := normalize(p)

	commit := dbx.NewCommitInfo(np)
	commit.Mode = &dbx.WriteMode{Tagged: dbxapi.Tagged{Tag: dbx.WriteModeOverwrite}}
	commit.Autorename = false
	if !meta.ModTime.IsZero() {
		commit.ClientModified = toDBXTime(meta.ModTime)
	}

	// 先頭を読んでみて、収まりきるなら1回の要求で送る。
	head, atEOF, err := readHead(r, smallUploadLimit)
	if err != nil {
		return nil, d.wrapErr("put", p, err)
	}

	var md *dbx.FileMetadata
	if atEOF {
		md, err = d.uploadSmall(ctx, commit, head)
	} else {
		md, err = d.uploadSession(ctx, commit, io.MultiReader(bytes.NewReader(head), r))
	}
	if err != nil {
		return nil, d.wrapErr("put", p, err)
	}

	fi := fileMetadataToFileInfo(md, parentOf(np))
	return &fi, nil
}

// uploadSmall は1回の要求で送りきります。
//
// 内容をすべて手元に持っているので content hash を添えられます。
// Dropbox 側で照合されるため、通信の途中で欠けた場合はここで失敗します。
func (d *Storage) uploadSmall(ctx context.Context, commit *dbx.CommitInfo, content []byte) (*dbx.FileMetadata, error) {
	arg := &dbx.UploadArg{
		CommitInfo:  *commit,
		ContentHash: contentHash(content),
	}
	// bytes.Reader は io.Seeker なので、SDK の再試行が効く。
	return d.client.UploadContext(ctx, arg, bytes.NewReader(content))
}

// uploadSession はアップロードセッションで分割して送ります。
//
// SDK の Uploader は確定済みのチャンクを保持したまま再試行するため、
// 読み直せない Reader でも一時的な失敗から復帰できます。
func (d *Storage) uploadSession(ctx context.Context, commit *dbx.CommitInfo, r io.Reader) (*dbx.FileMetadata, error) {
	src, err := filetransfer.ReaderUpload(r)
	if err != nil {
		return nil, err
	}

	res, err := d.uploader.Upload(ctx, src, commit, filetransfer.UploadOptions{
		MaxAttempts: uploadMaxAttempts,
	})
	if err != nil {
		return nil, err
	}
	return res.Metadata, nil
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
		// limit を超えた。分割して送る。
		return buf, false, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return buf[:n], true, nil
	default:
		return nil, false, err
	}
}

// contentHash は Dropbox の content hash を求めます。
func contentHash(content []byte) string {
	h := storage.NewDropboxContentHash()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// Mkdir はディレクトリを作ります。すでにあれば何もしません。
func (d *Storage) Mkdir(ctx context.Context, dir string) error {
	nd := normalize(dir)
	if nd == "" {
		// ルートは常にある。
		return nil
	}

	err := d.mkdirOne(ctx, nd)
	if err == nil {
		return nil
	}

	// 親がまだない場合に備える。
	//
	// Dropbox は親を自動で作ることになっているが、名前空間の設定に
	// よっては作られない。作れなかったときだけ親をさかのぼる。
	if isNotFound(err) {
		parent := parentOf(nd)
		if parent != nd {
			if err := d.Mkdir(ctx, parent); err != nil {
				return err
			}
			err = d.mkdirOne(ctx, nd)
			if err == nil {
				return nil
			}
		}
	}

	return d.wrapErr("mkdir", dir, err)
}

// mkdirOne は1階層ぶんのディレクトリを作ります。
// すでにディレクトリがある場合は成功として扱います。
func (d *Storage) mkdirOne(ctx context.Context, nd string) error {
	_, err := d.client.CreateFolderV2Context(ctx, dbx.NewCreateFolderArg(nd))
	if err == nil {
		return nil
	}
	if !isConflict(err) {
		return err
	}

	// すでに何かある。ディレクトリならそのままでよい。
	if strings.Contains(summaryOf(err), "conflict/file") {
		return fmt.Errorf("%w: 同じ名前のファイルがあります", storage.ErrExist)
	}
	return nil
}

// Remove は1つのファイル、または空のディレクトリを削除します。
//
// Dropbox の delete は中身ごと消してしまうため、ディレクトリの場合は
// 先に空であることを確かめます。中身ごと消したい場合は Purge を使ってください。
func (d *Storage) Remove(ctx context.Context, p string) error {
	if isRoot(p) {
		return d.wrapErr("remove", p, errors.New("ルートは削除できません"))
	}
	np := normalize(p)

	if err := d.ensureRemovable(ctx, np); err != nil {
		return d.wrapErr("remove", p, err)
	}

	_, err := d.client.DeleteV2Context(ctx, dbx.NewDeleteArg(np))
	return d.wrapErr("remove", p, err)
}

// ensureRemovable は、消してよい対象かどうかを確かめます。
func (d *Storage) ensureRemovable(ctx context.Context, np string) error {
	arg := dbx.NewListFolderArg(np)
	arg.Limit = 1

	res, err := d.client.ListFolderContext(ctx, arg)
	switch {
	case err == nil && len(res.Entries) > 0:
		return fmt.Errorf("%w: 中身ごと消すには purge を使ってください", storage.ErrNotEmpty)
	case err == nil:
		// 空のディレクトリ。
		return nil
	case strings.Contains(summaryOf(err), "not_folder"):
		// ファイルだった。そのまま消してよい。
		return nil
	default:
		return err
	}
}

// Purge はディレクトリを中身ごと削除します。
func (d *Storage) Purge(ctx context.Context, dir string) error {
	if isRoot(dir) {
		return d.wrapErr("purge", dir, errors.New("ルートは削除できません"))
	}
	_, err := d.client.DeleteV2Context(ctx, dbx.NewDeleteArg(normalize(dir)))
	return d.wrapErr("purge", dir, err)
}

// Hash はファイルの content hash を返します。
func (d *Storage) Hash(ctx context.Context, p string, ht storage.HashType) (string, error) {
	if ht != storage.DropboxContent {
		return "", fmt.Errorf("%w: dropbox が扱えるのは %s だけです（%s を要求されました）",
			storage.ErrUnsupported, storage.DropboxContent, ht)
	}

	md, err := d.client.GetMetadataContext(ctx, dbx.NewGetMetadataArg(normalize(p)))
	if err != nil {
		return "", d.wrapErr("hash", p, err)
	}

	file, ok := md.(*dbx.FileMetadata)
	if !ok {
		return "", d.wrapErr("hash", p, storage.ErrIsDir)
	}
	return file.ContentHash, nil
}

// ServerSideCopy は内容を転送せずにコピーします。
func (d *Storage) ServerSideCopy(ctx context.Context, srcPath, dstPath string) (*storage.FileInfo, error) {
	res, err := d.client.CopyV2Context(ctx,
		dbx.NewRelocationArg(normalize(srcPath), normalize(dstPath)))
	if err != nil {
		return nil, d.wrapErr("copy", srcPath, err)
	}

	fi, ok := toFileInfo(res.Metadata, parentOf(normalize(dstPath)))
	if !ok {
		return nil, d.wrapErr("copy", dstPath, errors.New("コピー結果を解釈できませんでした"))
	}
	return &fi, nil
}

// Move は内容を転送せずに移動・改名します。
func (d *Storage) Move(ctx context.Context, srcPath, dstPath string) error {
	from, to := normalize(srcPath), normalize(dstPath)

	_, err := d.client.MoveV2Context(ctx, dbx.NewRelocationArg(from, to))
	if err == nil {
		return nil
	}

	// 移動先にすでにある場合、Dropbox は自動で名前を変えるか失敗する。
	// 上書きの意図で呼ばれているので、どけてからやり直す。
	if isConflict(err) {
		if _, delErr := d.client.DeleteV2Context(ctx, dbx.NewDeleteArg(to)); delErr == nil {
			if _, err2 := d.client.MoveV2Context(ctx, dbx.NewRelocationArg(from, to)); err2 == nil {
				return nil
			}
		}
	}
	return d.wrapErr("move", srcPath, err)
}

// --- メタデータの変換 ---

// toFileInfo は Dropbox のメタデータを storage.FileInfo にします。
// ファイルでもディレクトリでもない記録の場合は false を返します。
func toFileInfo(md dbx.IsMetadata, parent string) (storage.FileInfo, bool) {
	switch m := md.(type) {
	case *dbx.FileMetadata:
		return fileMetadataToFileInfo(m, parent), true
	case *dbx.FolderMetadata:
		return storage.FileInfo{
			Path:  entryPath(m.PathDisplay, parent, m.Name),
			Name:  m.Name,
			IsDir: true,
			Size:  storage.SizeUnknown,
			ID:    m.Id,
		}, true
	}
	return storage.FileInfo{}, false
}

// fileMetadataToFileInfo はファイルのメタデータを storage.FileInfo にします。
func fileMetadataToFileInfo(m *dbx.FileMetadata, parent string) storage.FileInfo {
	fi := storage.FileInfo{
		Path:    entryPath(m.PathDisplay, parent, m.Name),
		Name:    m.Name,
		Size:    int64(m.Size),
		ModTime: fromDBXTime(m.ClientModified),
		ID:      m.Id,
	}
	if m.ContentHash != "" {
		fi.Hashes = map[storage.HashType]string{storage.DropboxContent: m.ContentHash}
	}
	return fi
}

// entryPath は一覧の1件のパスを決めます。
//
// Dropbox は登録時の大文字小文字を保った path_display を返すので、
// 取れるならそちらを使います。共有リンク越しなど、返ってこない
// 場面もあるため、その場合は問い合わせたディレクトリから組み立てます。
func entryPath(pathDisplay, parent, name string) string {
	if pathDisplay != "" {
		return pathDisplay
	}
	return path.Join(display(parent), name)
}

// toDBXTime は時刻を Dropbox が受け取れる形にします。
//
// Dropbox は UTC の秒までしか保持しないので、ここで丸めます。
// 丸めずに渡すと、書き込んだ直後に読み戻した時刻が変わって見え、
// 次回の比較で毎回コピーし直すことになります。
func toDBXTime(t time.Time) *dbxapi.DBXTime {
	d := dbxapi.DBXTime(t.UTC().Truncate(time.Second))
	return &d
}

// fromDBXTime は Dropbox の時刻を time.Time に戻します。
func fromDBXTime(t dbxapi.DBXTime) time.Time {
	return time.Time(t)
}

var (
	_ storage.Storage          = (*Storage)(nil)
	_ storage.Hasher           = (*Storage)(nil)
	_ storage.Purger           = (*Storage)(nil)
	_ storage.Mover            = (*Storage)(nil)
	_ storage.RangeOpener      = (*Storage)(nil)
	_ storage.ServerSideCopier = (*Storage)(nil)
)
