package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 適合性テストを試験用の WebDAV サーバーに対して実行します。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			_, _, s := newTestStorage(t, func(c *Config) { c.Preset = PresetNextcloud })

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		// 試験用のサーバーは中身をこの計算機のファイルシステムに置くので、
		// Windows では名前の中の逆斜線が区切りとして解釈されてしまいます。
		// 実物の WebDAV サーバーでは名前の一部として扱われます。
		IllegalNameChars: windowsOnlyIllegalChars(),
		LargeDirCount:    60,
	})
}

func windowsOnlyIllegalChars() string {
	if runtime.GOOS == "windows" {
		return `\`
	}
	return ""
}

func put(t *testing.T, ctx context.Context, s *Storage, p, content string) {
	t.Helper()
	if _, err := s.Put(ctx, p, strings.NewReader(content), storage.ObjectMeta{
		Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("Put(%s): %v", p, err)
	}
}

func readAll(t *testing.T, ctx context.Context, s *Storage, p string) string {
	t.Helper()
	rc, _, err := s.Open(ctx, p)
	if err != nil {
		t.Fatalf("Open(%s): %v", p, err)
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", p, err)
	}
	return string(b)
}

// 更新時刻を保持できるかどうかが、相手の種類で変わることを確かめます。
//
// WebDAV には時刻を書き換える標準の方法がありません。
// {DAV:}getlastmodified はサーバーが管理する項目なので、
// PROPPATCH では変えられないことになっています。
func TestModTimeDependsOnPreset(t *testing.T) {
	want := time.Date(2021, 6, 15, 12, 34, 56, 0, time.UTC)

	t.Run("nextcloud なら保持できる", func(t *testing.T) {
		ctx, _, s := newTestStorage(t, func(c *Config) { c.Preset = PresetNextcloud })

		if !s.Features().CanSetModTime {
			t.Fatal("保持できると申告していない")
		}
		if _, err := s.Put(ctx, "/時刻.txt", strings.NewReader("x"), storage.ObjectMeta{
			Size:    1,
			ModTime: want,
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		fi, err := s.Stat(ctx, "/時刻.txt")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if diff := fi.ModTime.Sub(want); diff > time.Second || diff < -time.Second {
			t.Errorf("更新時刻 = %v, want %v", fi.ModTime, want)
		}
	})

	t.Run("generic では保持できないと申告する", func(t *testing.T) {
		_, _, s := newTestStorage(t)

		// できないことをできると言わない。--compare modtime を
		// 指定されたら起動時に断れるようにするため。
		if s.Features().CanSetModTime {
			t.Error("保持できないのにできると申告している")
		}
	})
}

// 書き込みが不可分であることを確かめます。
//
// 別名で書いてから MOVE で置き換えるので、途中で失敗しても
// 中身の欠けたファイルが本来の場所に残りません。
func TestPutIsAtomic(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	broken := io.MultiReader(
		strings.NewReader(strings.Repeat("x", 100)),
		errReader{errors.New("読み取りに失敗しました")},
	)

	if _, err := s.Put(ctx, "/壊れる.txt", broken, storage.ObjectMeta{Size: 1000}); err == nil {
		t.Fatal("失敗するはずの書き込みが成功した")
	}

	if _, err := os.Stat(filepath.Join(f.root, "壊れる.txt")); !os.IsNotExist(err) {
		t.Error("中身の欠けたファイルが残っている")
	}

	// 書きかけの片付けは、相手が受け取り終える前に消しにいくことも
	// あるので確実ではない。残っても本来の場所は汚れないし、
	// 一覧にも出ない、というところまでを保証する。
	names := []string{}
	if err := s.List(ctx, "/", func(fi storage.FileInfo) error {
		names = append(names, fi.Name)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("一覧 = %v, 何も見えないはず", names)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// 書き込み中の一時ファイルが一覧に出ないことを確かめます。
func TestPartFilesAreHidden(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	if err := os.WriteFile(filepath.Join(f.root, ".のこり.txt"+partSuffix), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	put(t, ctx, s, "/ふつう.txt", "なかみ")

	names := []string{}
	if err := s.List(ctx, "/", func(fi storage.FileInfo) error {
		names = append(names, fi.Name)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(names) != 1 || names[0] != "ふつう.txt" {
		t.Errorf("一覧 = %v, want [ふつう.txt]", names)
	}
}

// 書き込みや問い合わせの転送先を追わないことを確かめます。
//
// PUT が転送されると、送り直しのときに GET になってしまう
// ことがあります。中身が消えかねないので追いません。
func TestDoesNotFollowRedirectsForWrites(t *testing.T) {
	tests := map[string]bool{
		http.MethodGet:    true,
		http.MethodHead:   true,
		http.MethodPut:    false,
		"PROPFIND":        false,
		"MKCOL":           false,
		"MOVE":            false,
		"COPY":            false,
		http.MethodDelete: false,
	}
	for method, want := range tests {
		if got := mayFollowRedirect(method); got != want {
			t.Errorf("mayFollowRedirect(%s) = %v, want %v", method, got, want)
		}
	}
}

// 同じディレクトリへ続けて書いても、MKCOL が増えないことを確かめます。
//
// 書き込みのたびにディレクトリを作りにいくと、要求が倍になるうえ
// 並行して書いたときに衝突します。
func TestDirectoryIsRememberedBetweenWrites(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	if err := s.Mkdir(ctx, "/入れ物"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	before := f.callCount("MKCOL")

	for i := range 5 {
		put(t, ctx, s, fmt.Sprintf("/入れ物/%d.txt", i), "x")
	}

	if got := f.callCount("MKCOL") - before; got != 0 {
		t.Errorf("書き込み中の MKCOL = %d回, want 0（一度作ったら覚えていること）", got)
	}
}

// Remove が中身ごと消してしまわないことを確かめます。
//
// WebDAV の DELETE はディレクトリを中身ごと消します。
func TestRemoveRefusesNonEmptyDir(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/消さない/中身.txt", "だいじ")

	err := s.Remove(ctx, "/消さない")
	if !errors.Is(err, storage.ErrNotEmpty) {
		t.Fatalf("Remove = %v, want ErrNotEmpty", err)
	}
	if _, err := s.Stat(ctx, "/消さない/中身.txt"); err != nil {
		t.Errorf("中身が消えている: %v", err)
	}

	if err := s.Purge(ctx, "/消さない"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, err := s.Stat(ctx, "/消さない"); !storage.IsNotFound(err) {
		t.Errorf("Purge のあとも残っている: %v", err)
	}
}

// root を指定すると、その下が起点になることを確かめます。
func TestRootIsApplied(t *testing.T) {
	f := newFakeWebDAV(t.TempDir())
	if err := os.MkdirAll(filepath.Join(f.root, "起点"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	s := f.start(t, func(c *Config) { c.Root = "起点" })
	ctx := context.Background()

	put(t, ctx, s, "/中身.txt", "なかみ")

	if _, err := os.Stat(filepath.Join(f.root, "起点", "中身.txt")); err != nil {
		t.Errorf("起点の下にない: %v", err)
	}
	if got := readAll(t, ctx, s, "/中身.txt"); got != "なかみ" {
		t.Errorf("内容 = %q", got)
	}
}

// サーバー側でのコピーと移動を確かめます。
func TestServerSideCopyAndMove(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/元.txt", "なかみ")

	if _, err := s.ServerSideCopy(ctx, "/元.txt", "/入れ物/複製.txt"); err != nil {
		t.Fatalf("ServerSideCopy: %v", err)
	}
	if got := readAll(t, ctx, s, "/入れ物/複製.txt"); got != "なかみ" {
		t.Errorf("複製の内容 = %q", got)
	}
	if got := readAll(t, ctx, s, "/元.txt"); got != "なかみ" {
		t.Error("コピーなのに元が消えている")
	}

	if err := s.Move(ctx, "/入れ物/複製.txt", "/移動先.txt"); err != nil {
		t.Fatalf("Move: %v", err)
	}
	if _, err := s.Stat(ctx, "/入れ物/複製.txt"); !storage.IsNotFound(err) {
		t.Error("移動なのに元が残っている")
	}
}

// 合言葉が違えば断られることを確かめます。
func TestAuthFailure(t *testing.T) {
	f := newFakeWebDAV(t.TempDir())
	s := f.start(t, func(c *Config) { c.Password = "ちがう" })

	_, err := s.Stat(context.Background(), "/どこか.txt")
	if class := storage.ClassOf(err); class != storage.ClassAuth {
		t.Errorf("失敗の種類 = %v, want auth", class)
	}
	if storage.ClassOf(err).Retryable() {
		t.Error("認証の失敗が再試行の対象になっている")
	}
}

// 誰かが編集中のときは、待って試し直す対象になることを確かめます。
func TestLockedIsRetryable(t *testing.T) {
	ctx, f, s := newTestStorage(t)
	f.failNext(http.MethodPut, 1, http.StatusLocked)

	_, err := s.Put(ctx, "/使用中.txt", strings.NewReader("x"), storage.ObjectMeta{Size: 1})
	if err == nil {
		t.Fatal("失敗するはずが成功した")
	}
	if class := storage.ClassOf(err); class != storage.ClassRetryable {
		t.Errorf("失敗の種類 = %v, want retryable", class)
	}
}

func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		status   int
		sentinel error
		class    storage.Class
	}{
		{http.StatusNotFound, storage.ErrNotFound, storage.ClassPermanent},
		{http.StatusGone, storage.ErrNotFound, storage.ClassPermanent},
		{http.StatusUnauthorized, nil, storage.ClassAuth},
		{http.StatusForbidden, nil, storage.ClassAuth},
		// 親がない場所に作ろうとすると 409 が返る。
		{http.StatusConflict, storage.ErrNotFound, storage.ClassPermanent},
		{http.StatusMethodNotAllowed, storage.ErrExist, storage.ClassPermanent},
		{http.StatusLocked, nil, storage.ClassRetryable},
		{http.StatusTooManyRequests, nil, storage.ClassRateLimit},
		{http.StatusInsufficientStorage, nil, storage.ClassPermanent},
		{http.StatusBadGateway, nil, storage.ClassRetryable},
	}
	for _, tt := range tests {
		got := classifyStatus(tt.status)
		if !errors.Is(got.sentinel, tt.sentinel) {
			t.Errorf("classifyStatus(%d) の番兵 = %v, want %v", tt.status, got.sentinel, tt.sentinel)
		}
		if got.class != tt.class {
			t.Errorf("classifyStatus(%d) の種類 = %v, want %v", tt.status, got.class, tt.class)
		}
	}
}

// 設定の誤りは接続を試みる前に知らせることを確かめます。
func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"入口がない", Config{}, "url"},
		{"入口の書き方が違う", Config{URL: "例.invalid/dav"}, "http://"},
		{"知らない相手の種類", Config{URL: "https://例.invalid", Preset: "どこか"}, "preset"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if err == nil {
				t.Fatal("誤りなのに通ってしまった")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("どこが悪いのか分からない: %v", err)
			}
		})
	}
}

func TestCleanPath(t *testing.T) {
	tests := map[string]string{
		"":          "/",
		"/":         "/",
		"a/b":       "/a/b",
		"/a//b":     "/a/b",
		"/a/b/../c": "/a/c",
		// 逆斜線は区切りではなく、名前の一部として扱う。
		"/a\\b": "/a\\b",
	}
	for in, want := range tests {
		if got := cleanPath(in); got != want {
			t.Errorf("cleanPath(%q) = %q, want %q", in, got, want)
		}
	}
}
