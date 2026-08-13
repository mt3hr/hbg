package smb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 試験用のファイル操作です。
//
// SMB のサーバーを Go で立てる手立てがないので、共有の中身を
// この計算機のディレクトリで代用します。これで確かめられるのは
// hbg 側の組み立て、つまり
//
//   - パスの組み立てと起点の適用
//   - 別名で書いてから置き換える手順
//   - 空でないディレクトリを消さない判断
//   - 書き込み中のものを一覧に出さないこと
//   - 失敗の分類
//
// です。SMB の手続き・認証・文字符号は go-smb2 の受け持ちなので、
// ここでは試験しません。実物に対する試験は smb_live_test.go にあります。

// localFS は共有の代わりにこの計算機のディレクトリを使います。
type localFS struct {
	root string

	mu sync.Mutex
	// failures は操作ごとの「あと何回失敗させるか」です。
	failures map[string]*fakeFailure
}

// localShare は取り消しの合図を伴った localFS です。
// 蓄えは元と共有します。
type localShare struct {
	*localFS
	ctx context.Context
}

type fakeFailure struct {
	remaining int
	err       error
}

func newLocalFS(root string) *localFS {
	return &localFS{
		root:     root,
		failures: map[string]*fakeFailure{},
	}
}

// failNext は次の n 回、その操作を失敗させます。
func (l *localFS) failNext(op string, n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failures[op] = &fakeFailure{remaining: n, err: err}
}

// check は失敗を注入すべきかを見ます。
func (l *localFS) check(op string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if fail, ok := l.failures[op]; ok && fail.remaining > 0 {
		fail.remaining--
		return fail.err
	}
	return nil
}

// abs は共有の中のパスを、この計算機のパスに直します。
func (l *localFS) abs(name string) string {
	name = strings.TrimPrefix(strings.ReplaceAll(name, "\\", "/"), "/")
	return filepath.Join(l.root, filepath.FromSlash(name))
}

func (l *localFS) WithContext(ctx context.Context) fileSystem {
	return &localShare{localFS: l, ctx: ctx}
}

func (l *localFS) Create(name string) (file, error) {
	if err := l.check("create"); err != nil {
		return nil, err
	}
	f, err := os.Create(l.abs(name))
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *localFS) Open(name string) (file, error) {
	if err := l.check("open"); err != nil {
		return nil, err
	}
	f, err := os.Open(l.abs(name))
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *localFS) Stat(name string) (os.FileInfo, error) {
	if err := l.check("stat"); err != nil {
		return nil, err
	}
	return os.Stat(l.abs(name))
}

func (l *localFS) ReadDir(name string) ([]os.FileInfo, error) {
	if err := l.check("readdir"); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(l.abs(name))
	if err != nil {
		return nil, err
	}

	out := make([]os.FileInfo, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

func (l *localFS) Mkdir(name string, perm os.FileMode) error {
	if err := l.check("mkdir"); err != nil {
		return err
	}
	return os.Mkdir(l.abs(name), perm)
}

func (l *localFS) Remove(name string) error {
	if err := l.check("remove"); err != nil {
		return err
	}
	return os.Remove(l.abs(name))
}

func (l *localFS) Rename(oldpath, newpath string) error {
	if err := l.check("rename"); err != nil {
		return err
	}
	// SMB の改名は置き換え先があると失敗する。実物に合わせる。
	if _, err := os.Stat(l.abs(newpath)); err == nil {
		return &os.LinkError{
			Op:  "rename",
			Old: oldpath,
			New: newpath,
			Err: os.ErrExist,
		}
	}
	return os.Rename(l.abs(oldpath), l.abs(newpath))
}

func (l *localFS) Chtimes(name string, atime, mtime time.Time) error {
	if err := l.check("chtimes"); err != nil {
		return err
	}
	return os.Chtimes(l.abs(name), atime, mtime)
}

func (l *localFS) Close() error { return nil }

// newTestStorage は試験用のストレージを作ります。
func newTestStorage(t *testing.T, mutate ...func(*Config)) (context.Context, *localFS, *Storage) {
	t.Helper()

	dir := t.TempDir()
	fs := newLocalFS(dir)

	cfg := Config{
		Name:       "偽smb",
		Host:       "example.invalid",
		Share:      "共有",
		fsOverride: fs,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("ストレージを作れません: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return context.Background(), fs, s
}
