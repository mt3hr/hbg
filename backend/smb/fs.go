package smb

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cloudsoda/go-smb2"
)

// SMB の共有はファイルシステムそのものなので、hbg 側でやることは
// 「どう読み書きするか」の組み立てだけです。実際の通信は go-smb2 が
// 受け持ちます。
//
// その境目をここで切っておきます。こうしておくと、SMB サーバーを
// 用意しなくても、パスの組み立て・別名で書いてから置き換える手順・
// 空でないディレクトリを消さない判断といった hbg 側の振る舞いを
// 適合性スイートで確かめられます。
//
// 通信そのもの（SMB の手続き・認証・文字符号）は go-smb2 の受け持ちなので、
// ここでは試験しません。実物に対する試験は integration の印を付けた
// ほうにあります。

// fileSystem は共有の中のファイル操作です。
type fileSystem interface {
	// WithContext は取り消しの合図を伝えた共有を返します。
	WithContext(ctx context.Context) fileSystem

	Create(name string) (file, error)
	Open(name string) (file, error)
	Stat(name string) (os.FileInfo, error)
	ReadDir(name string) ([]os.FileInfo, error)
	Mkdir(name string, perm os.FileMode) error
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Chtimes(name string, atime, mtime time.Time) error
	Close() error
}

// file は開いたファイルです。
type file interface {
	io.ReadWriteCloser
	io.Seeker
	Stat() (os.FileInfo, error)
}

// smbShare は go-smb2 の共有を fileSystem に合わせます。
type smbShare struct {
	share *smb2.Share
}

func (s smbShare) WithContext(ctx context.Context) fileSystem {
	return smbShare{share: s.share.WithContext(ctx)}
}

func (s smbShare) Create(name string) (file, error) {
	f, err := s.share.Create(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s smbShare) Open(name string) (file, error) {
	f, err := s.share.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s smbShare) Stat(name string) (os.FileInfo, error) { return s.share.Stat(name) }

func (s smbShare) ReadDir(name string) ([]os.FileInfo, error) { return s.share.ReadDir(name) }

func (s smbShare) Mkdir(name string, perm os.FileMode) error { return s.share.Mkdir(name, perm) }

func (s smbShare) Remove(name string) error { return s.share.Remove(name) }

func (s smbShare) Rename(oldpath, newpath string) error { return s.share.Rename(oldpath, newpath) }

func (s smbShare) Chtimes(name string, atime, mtime time.Time) error {
	return s.share.Chtimes(name, atime, mtime)
}

func (s smbShare) Close() error { return s.share.Umount() }
