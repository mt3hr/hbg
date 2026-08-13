package googledrive

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/mt3hr/hbg/storage"
	drive "google.golang.org/api/drive/v3"
)

// Google Drive はパスの木ではなく、IDの網です。
// フォルダは親への参照を持つだけで、"/a/b/c.txt" のような場所の指定は
// API にありません。したがって、パスを使うには根から1段ずつ
// 名前で引き当てていく必要があります。
//
// 以前の実装には2つの問題がありました。
//
//   - 1段ごとに、そのフォルダの中身を全件列挙して名前を突き合わせていた。
//     件数の多いフォルダでは、階層をたどるだけで大量のやりとりが発生する。
//
//   - 途中の段が見つからないとき、それを黙って読み飛ばし、
//     ひとつ上の階層をそのまま結果として返していた。存在しない場所を
//     指定したのにエラーにならず、別のディレクトリを対象にしてしまう。
//
// ここでは名前で絞り込んだ問い合わせを1段につき1回行い、
// 見つからなければその場で失敗させます。解決したディレクトリのIDは
// 覚えておくので、同じ木の中を歩くあいだは問い合わせが増えません。

// resolver はパスから Drive のIDを求めます。
type resolver struct {
	s *Storage

	mu sync.RWMutex
	// dirs は正規化したディレクトリのパスからIDへの対応です。
	// ファイルは覚えません。書き換えられると古いIDを掴むためです。
	dirs map[string]string
}

func newResolver(s *Storage) *resolver {
	return &resolver{
		s:    s,
		dirs: map[string]string{"/": s.rootID},
	}
}

// dirID はディレクトリのIDを返します。見つからなければ ErrNotFound です。
func (r *resolver) dirID(ctx context.Context, dir string) (string, error) {
	return r.walk(ctx, cleanPath(dir), false)
}

// dirIDCreating はディレクトリのIDを返します。途中の段がなければ作ります。
func (r *resolver) dirIDCreating(ctx context.Context, dir string) (string, error) {
	return r.walk(ctx, cleanPath(dir), true)
}

// walk は根から1段ずつたどります。
func (r *resolver) walk(ctx context.Context, dir string, create bool) (string, error) {
	if id, ok := r.cached(dir); ok {
		return id, nil
	}

	parent := path.Dir(dir)
	parentID, err := r.walk(ctx, parent, create)
	if err != nil {
		return "", err
	}

	name := path.Base(dir)
	file, err := r.s.findChild(ctx, parentID, name)
	if err != nil {
		return "", err
	}

	switch {
	case file == nil && !create:
		return "", fmt.Errorf("%w: ディレクトリ %s", storage.ErrNotFound, dir)
	case file == nil:
		file, err = r.s.createFolder(ctx, parentID, name)
		if err != nil {
			return "", err
		}
	case file.MimeType != folderMIME:
		return "", fmt.Errorf("%w: %s", storage.ErrNotDir, dir)
	}

	r.remember(dir, file.Id)
	return file.Id, nil
}

func (r *resolver) cached(dir string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.dirs[dir]
	return id, ok
}

func (r *resolver) remember(dir, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dirs[dir] = id
}

// forget は、そのパスと配下の記憶を捨てます。
// 削除や移動のあとに古いIDを掴まないようにするためのものです。
func (r *resolver) forget(p string) {
	p = cleanPath(p)

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.dirs, p)
	prefix := strings.TrimSuffix(p, "/") + "/"
	for k := range r.dirs {
		if strings.HasPrefix(k, prefix) {
			delete(r.dirs, k)
		}
	}
}

// file はパスに対応するファイル（またはフォルダ）を返します。
func (r *resolver) file(ctx context.Context, p string) (*drive.File, error) {
	p = cleanPath(p)
	if p == "/" {
		return &drive.File{Id: r.s.rootID, Name: "/", MimeType: folderMIME}, nil
	}

	parentID, err := r.dirID(ctx, path.Dir(p))
	if err != nil {
		return nil, err
	}

	file, err := r.s.findChild(ctx, parentID, path.Base(p))
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, p)
	}
	return file, nil
}

// cleanPath はパスを正規化します。区切りは "/"、先頭は "/" です。
//
// "\\" は区切りとして扱いません。Drive はファイル名に含められる
// ふつうの文字なので、区切りに読み替えると "a\b.txt" という名前の
// ファイルが "a" の下の "b.txt" になってしまいます。
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

// escapeQuery は検索式に埋め込む文字列を安全にします。
//
// 引用符を含むファイル名をそのまま埋めると検索式が壊れ、
// 意図しない条件で検索されることになります。
func escapeQuery(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}
