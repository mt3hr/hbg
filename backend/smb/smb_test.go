package smb

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 適合性テストを、共有の代わりにこの計算機のディレクトリで実行します。
//
// 確かめているのは hbg 側の組み立てです。SMB の通信そのものは
// go-smb2 の受け持ちなので、ここには含まれません。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			_, _, s := newTestStorage(t)

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		// 共有の中身をこの計算機のファイルシステムで代用しているため、
		// 名前の中の逆斜線は区切りとして解釈されてしまいます。
		// もっとも SMB は Windows の作法に従うので、逆斜線は
		// そもそもファイル名に使えません（Features.IllegalChars 参照）。
		IllegalNameChars: `\`,
	})
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

// 書き込みが不可分であることを確かめます。
func TestPutIsAtomic(t *testing.T) {
	ctx, fs, s := newTestStorage(t)

	broken := io.MultiReader(
		strings.NewReader(strings.Repeat("x", 100)),
		errReader{errors.New("読み取りに失敗しました")},
	)

	if _, err := s.Put(ctx, "/壊れる.txt", broken, storage.ObjectMeta{Size: 1000}); err == nil {
		t.Fatal("失敗するはずの書き込みが成功した")
	}

	entries, err := os.ReadDir(fs.root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("後始末されていないファイルが残っている: %s", e.Name())
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// すでにあるファイルを置き換えられることを確かめます。
//
// SMB の改名は置き換え先があると失敗します。先にどけてから移します。
func TestPutOverwrites(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	put(t, ctx, s, "/上書き.txt", "ふるい")
	put(t, ctx, s, "/上書き.txt", "あたらしい")

	if got := readAll(t, ctx, s, "/上書き.txt"); got != "あたらしい" {
		t.Errorf("内容 = %q", got)
	}
}

// 書き込み中の一時ファイルが一覧に出ないことを確かめます。
func TestPartFilesAreHidden(t *testing.T) {
	ctx, fs, s := newTestStorage(t)

	// 前回の中断で残ったものを模す。
	if err := os.WriteFile(filepath.Join(fs.root, ".のこり.txt"+partSuffix), []byte("x"), 0o600); err != nil {
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

// Remove が中身ごと消してしまわないことを確かめます。
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
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "起点"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	fs := newLocalFS(dir)
	s, err := New(context.Background(), Config{
		Name:       "起点つき",
		Host:       "example.invalid",
		Share:      "共有",
		Root:       "起点",
		fsOverride: fs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	put(t, ctx, s, "/中身.txt", "なかみ")

	if _, err := os.Stat(filepath.Join(dir, "起点", "中身.txt")); err != nil {
		t.Errorf("起点の下にない: %v", err)
	}
	if got := readAll(t, ctx, s, "/中身.txt"); got != "なかみ" {
		t.Errorf("内容 = %q", got)
	}
}

// 更新時刻が保たれることを確かめます。
func TestModTimeRoundTrip(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	want := time.Date(2021, 6, 15, 12, 34, 56, 0, time.UTC)
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
	if !fi.ModTime.Equal(want) {
		t.Errorf("更新時刻 = %v, want %v", fi.ModTime, want)
	}
}

// 相手が使用中のときは、待って試し直す対象になることを確かめます。
func TestSharingViolationIsRetryable(t *testing.T) {
	ctx, fs, s := newTestStorage(t)
	fs.failNext("create", 1, errors.New("smb2: STATUS_SHARING_VIOLATION"))

	_, err := s.Put(ctx, "/使用中.txt", strings.NewReader("x"), storage.ObjectMeta{Size: 1})
	if err == nil {
		t.Fatal("失敗するはずが成功した")
	}
	if class := storage.ClassOf(err); class != storage.ClassRetryable {
		t.Errorf("失敗の種類 = %v, want retryable", class)
	}
}

// 権限の問題は再試行せず、処理全体を止めることを確かめます。
func TestAccessDeniedIsFatal(t *testing.T) {
	ctx, fs, s := newTestStorage(t)
	fs.failNext("stat", 1, errors.New("smb2: STATUS_ACCESS_DENIED"))

	_, err := s.Stat(ctx, "/どこか.txt")
	if class := storage.ClassOf(err); class != storage.ClassAuth {
		t.Errorf("失敗の種類 = %v, want auth", class)
	}
	if storage.ClassOf(err).Retryable() {
		t.Error("権限の問題が再試行の対象になっている")
	}
}

func TestClassifyStatusName(t *testing.T) {
	tests := []struct {
		message  string
		sentinel error
		class    storage.Class
	}{
		{"smb2: STATUS_OBJECT_NAME_NOT_FOUND", storage.ErrNotFound, storage.ClassPermanent},
		{"smb2: STATUS_OBJECT_PATH_NOT_FOUND", storage.ErrNotFound, storage.ClassPermanent},
		{"smb2: STATUS_DIRECTORY_NOT_EMPTY", storage.ErrNotEmpty, storage.ClassPermanent},
		{"smb2: STATUS_OBJECT_NAME_COLLISION", storage.ErrExist, storage.ClassPermanent},
		{"smb2: STATUS_LOGON_FAILURE", nil, storage.ClassAuth},
		{"smb2: STATUS_DISK_FULL", nil, storage.ClassPermanent},
		{"smb2: STATUS_SHARING_VIOLATION", nil, storage.ClassRetryable},
		{"何か知らない失敗", nil, storage.ClassUnknown},
	}
	for _, tt := range tests {
		got := classifyStatusName(tt.message)
		if !errors.Is(got.sentinel, tt.sentinel) {
			t.Errorf("%q の番兵 = %v, want %v", tt.message, got.sentinel, tt.sentinel)
		}
		if got.class != tt.class {
			t.Errorf("%q の種類 = %v, want %v", tt.message, got.class, tt.class)
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
		{"接続先がない", Config{Share: "共有"}, "host"},
		{"共有名がない", Config{Host: "計算機"}, "share"},
		{
			// Windows の書き方をそのまま渡されたとき、
			// どう書けばよいのかを伝える。
			name: "UNC の書き方",
			cfg:  Config{Host: `\\計算機\共有`, Share: "共有"},
			want: "host と share に分けて",
		},
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
		"/a/./b":    "/a/b",
		"/a/b/../c": "/a/c",
		// SMB は Windows の作法に従うので、逆斜線は区切りとして読み替える。
		"\\a\\b": "/a/b",
	}
	for in, want := range tests {
		if got := cleanPath(in); got != want {
			t.Errorf("cleanPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTempPath(t *testing.T) {
	tests := map[string]string{
		"a/b.txt": "a/.b.txt" + partSuffix,
		"b.txt":   ".b.txt" + partSuffix,
	}
	for in, want := range tests {
		if got := tempPath(in); got != want {
			t.Errorf("tempPath(%q) = %q, want %q", in, got, want)
		}
	}
}
