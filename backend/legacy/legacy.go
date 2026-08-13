// Package legacy は、旧来の hbg.Storage を新しい storage.Storage として
// 使えるようにします。
//
// Dropbox と Google Drive の実装は段階的に作り直す予定なので、
// それまでの間、新しいインターフェースを前提とした転送エンジンから
// 旧実装を使えるようにするための足場です。
//
// 旧実装は context を受け取らないため、取り消しは操作の切れ目でしか
// 効きません。作り直しが済んだらこのパッケージは不要になります。
package legacy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mt3hr/hbg"
	"github.com/mt3hr/hbg/storage"
)

// Adapter は旧 hbg.Storage を storage.Storage として見せます。
type Adapter struct {
	inner    hbg.Storage
	features *storage.Features
}

// Wrap は旧 Storage を新しいインターフェースに適合させます。
func Wrap(inner hbg.Storage, features *storage.Features) *Adapter {
	if features == nil {
		features = &storage.Features{
			ModTimePrecision: time.Second,
			CanSetModTime:    true,
			EmptyDirs:        true,
			ImplicitDirs:     true,
		}
	}
	return &Adapter{inner: inner, features: features}
}

// Unwrap は元の Storage を返します。
func (a *Adapter) Unwrap() hbg.Storage { return a.inner }

// Type はストレージの種別を返します。
func (a *Adapter) Type() string { return a.inner.Type() }

// Name は設定ファイルで付けた名前を返します。
func (a *Adapter) Name() string { return a.inner.Name() }

// Features はこのストレージにできることを返します。
func (a *Adapter) Features() *storage.Features { return a.features }

// Close はストレージを閉じます。
func (a *Adapter) Close() error { return a.inner.Close() }

// wrapErr は旧実装のエラーを storage のエラーに変換します。
//
// 旧実装は失敗を分類しませんが、下層の os やライブラリのエラーは
// 包まれたまま残っているので、そこから判定できるものは判定します。
// 判定できないものは ClassUnknown として保守的に扱います
// （再試行の対象になります）。
func (a *Adapter) wrapErr(op, path string, err error) error {
	if err == nil {
		return nil
	}

	class := storage.ClassUnknown
	switch {
	case errors.Is(err, os.ErrNotExist):
		// 呼び出し側が storage.ErrNotFound で判定できるようにする。
		// 元のエラーも失わないよう両方を包む。
		err = fmt.Errorf("%w (%w)", storage.ErrNotFound, err)
		class = storage.ClassPermanent
	case errors.Is(err, os.ErrPermission):
		class = storage.ClassPermanent
	case errors.Is(err, os.ErrExist):
		err = fmt.Errorf("%w (%w)", storage.ErrExist, err)
		class = storage.ClassPermanent
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		class = storage.ClassCanceled
	}
	return storage.Wrap(op, a.inner.Name(), path, class, err)
}

func toFileInfo(fi *hbg.FileInfo) storage.FileInfo {
	out := storage.FileInfo{
		Path:    fi.Path,
		Name:    fi.Name,
		IsDir:   fi.IsDir,
		Size:    fi.Size,
		ModTime: fi.LastMod,
	}
	if fi.IsDir {
		out.Size = storage.SizeUnknown
	}
	return out
}

// List はディレクトリの中身を1件ずつ fn に渡します。
//
// 旧実装は全件をまとめて返すため、ここではメモリ一定にはなりません。
// 作り直しの際に解消されます。
func (a *Adapter) List(ctx context.Context, dir string, fn func(storage.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return a.canceled("list", dir, err)
	}

	infos, err := a.inner.List(dir)
	if err != nil {
		return a.wrapErr("list", dir, err)
	}

	for _, fi := range infos {
		if err := ctx.Err(); err != nil {
			return a.canceled("list", dir, err)
		}
		if err := fn(toFileInfo(fi)); err != nil {
			return err
		}
	}
	return nil
}

// Stat は1件のメタデータを返します。
func (a *Adapter) Stat(ctx context.Context, path string) (*storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, a.canceled("stat", path, err)
	}

	fi, err := a.inner.Stat(path)
	if err != nil {
		return nil, a.wrapErr("stat", path, err)
	}
	out := toFileInfo(fi)
	return &out, nil
}

// Open はファイルの内容を読む ReadCloser を返します。
func (a *Adapter) Open(ctx context.Context, path string) (io.ReadCloser, *storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, a.canceled("open", path, err)
	}

	file, err := a.inner.Get(path)
	if err != nil {
		return nil, nil, a.wrapErr("open", path, err)
	}

	info := &storage.FileInfo{
		Path:    path,
		Name:    file.Name,
		Size:    file.Size,
		ModTime: file.LastMod,
	}
	return &ctxReadCloser{ctx: ctx, rc: file.Data}, info, nil
}

// ctxReadCloser は読み取りのたびに ctx を確認します。
// 旧実装は context を受け取らないので、せめて読み取りの区切りで
// 取り消しが効くようにします。
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
func (a *Adapter) Put(ctx context.Context, path string, r io.Reader, meta storage.ObjectMeta) (*storage.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, a.canceled("put", path, err)
	}

	dir, name := splitPath(path)

	// 旧 Push は io.ReadCloser を要求するが、内容を閉じるのは
	// 呼び出し側の責任なので、閉じない包みを渡す。
	counting := &countingReader{r: &ctxReader{ctx: ctx, r: r}}
	file := &hbg.File{
		Data:    io.NopCloser(counting),
		Name:    name,
		LastMod: meta.ModTime,
		Size:    meta.Size,
	}

	if err := a.inner.Push(dir, file); err != nil {
		return nil, a.wrapErr("put", path, err)
	}

	return &storage.FileInfo{
		Path:    path,
		Name:    name,
		Size:    counting.n,
		ModTime: meta.ModTime,
	}, nil
}

// countingReader は読み取ったバイト数を数えます。
// 旧 Push は書き込んだ量を返さないので、ここで測ります。
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

// Mkdir はディレクトリを作ります。
func (a *Adapter) Mkdir(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return a.canceled("mkdir", dir, err)
	}
	return a.wrapErr("mkdir", dir, a.inner.MkDir(dir))
}

// Remove は削除します。
//
// 旧 Delete は中身ごと消すので、ここでは空ディレクトリかどうかを
// 区別できません。作り直しの際に分離されます。
func (a *Adapter) Remove(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return a.canceled("remove", path, err)
	}
	return a.wrapErr("remove", path, a.inner.Delete(path))
}

// Purge はディレクトリを中身ごと削除します。
func (a *Adapter) Purge(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return a.canceled("purge", dir, err)
	}
	return a.wrapErr("purge", dir, a.inner.Delete(dir))
}

func (a *Adapter) canceled(op, path string, err error) error {
	return storage.Wrap(op, a.inner.Name(), path, storage.ClassCanceled, err)
}

// splitPath はパスをディレクトリと名前に分けます。
//
// storage.CleanPath は使いません。あちらはストレージのルートを起点とした
// パスを前提に先頭へ "/" を補うため、ローカルファイルシステムのように
// OS のパス規則に従うストレージでは C:/... が /C:/... に壊れてしまいます。
// ここでは区切り文字を揃えるだけにとどめます。
func splitPath(p string) (dir, name string) {
	p = strings.ReplaceAll(p, "\\", "/")

	i := strings.LastIndex(p, "/")
	switch {
	case i < 0:
		return ".", p
	case i == 0:
		return "/", p[1:]
	default:
		return p[:i], p[i+1:]
	}
}

var (
	_ storage.Storage = (*Adapter)(nil)
	_ storage.Purger  = (*Adapter)(nil)
)
