package ftp

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 適合性テストを試験用の FTP サーバーに対して実行します。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			f := startFakeFTP(t)
			s := f.connect(t)

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		// 試験用のサーバーは中身をこの計算機のファイルシステムに置くので、
		// Windows では名前の中の逆斜線が区切りとして解釈されてしまいます。
		IllegalNameChars: windowsOnlyIllegalChars(),
		// FTP はやりとりが多いので、件数を抑えます。
		LargeDirCount: 40,
	})
}

func windowsOnlyIllegalChars() string {
	if runtime.GOOS == "windows" {
		return `\`
	}
	return ""
}

func newTestStorage(t *testing.T) (context.Context, *fakeFTP, *Storage) {
	t.Helper()
	f := startFakeFTP(t)
	return context.Background(), f, f.connect(t)
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

// 更新時刻が保たれることを確かめます。
//
// 以前の実装は時刻をまったく扱っておらず、ヘルプにも
// 「タイムスタンプは消滅します」と書かれていました。
// MFMT に応じる相手なら保持できます。
func TestModTimeIsKept(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	if !s.Features().CanSetModTime {
		t.Fatal("MFMT に応じる相手なのに、保持できないと申告している")
	}

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
	if diff := fi.ModTime.Sub(want); diff > time.Second || diff < -time.Second {
		t.Errorf("更新時刻 = %v, want %v", fi.ModTime, want)
	}

	// あとから変えられること。
	later := want.Add(48 * time.Hour)
	if err := s.SetModTime(ctx, "/時刻.txt", later); err != nil {
		t.Fatalf("SetModTime: %v", err)
	}
	fi, err = s.Stat(ctx, "/時刻.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if diff := fi.ModTime.Sub(later); diff > time.Second || diff < -time.Second {
		t.Errorf("変更後の更新時刻 = %v, want %v", fi.ModTime, later)
	}
}

// MFMT に応じない相手には、保持できないと申告することを確かめます。
//
// できないことをできると言うと、--compare modtime を指定したときに
// 毎回コピーし直すことになります。
func TestModTimeUnsupported(t *testing.T) {
	f := startFakeFTP(t, func(f *fakeFTP) { f.setTimeSupported = false })
	s := f.connect(t)

	if s.Features().CanSetModTime {
		t.Error("MFMT に応じない相手なのに、保持できると申告している")
	}

	err := s.SetModTime(context.Background(), "/どこか.txt", time.Now())
	if !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("SetModTime = %v, want ErrUnsupported", err)
	}
}

// 書き込みが不可分であることを確かめます。
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

// すでにあるファイルを置き換えられることを確かめます。
//
// FTP の改名は置き換え先があると失敗します。先にどけてから移します。
func TestPutOverwrites(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	put(t, ctx, s, "/上書き.txt", "ふるい")
	put(t, ctx, s, "/上書き.txt", "あたらしい")

	if got := readAll(t, ctx, s, "/上書き.txt"); got != "あたらしい" {
		t.Errorf("内容 = %q", got)
	}
}

// 途中から読み出せることを確かめます。
func TestOpenRange(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/範囲.txt", "0123456789")

	tests := []struct {
		offset, length int64
		want           string
	}{
		{0, 3, "012"},
		{4, 3, "456"},
		{7, -1, "789"},
	}
	for _, tt := range tests {
		rc, err := s.OpenRange(ctx, "/範囲.txt", tt.offset, tt.length)
		if err != nil {
			t.Fatalf("OpenRange(%d,%d): %v", tt.offset, tt.length, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != tt.want {
			t.Errorf("OpenRange(%d,%d) = %q, want %q", tt.offset, tt.length, got, tt.want)
		}
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

// root を指定すると、その下が起点になることを確かめます。
func TestRootIsApplied(t *testing.T) {
	f := startFakeFTP(t)
	if err := os.MkdirAll(filepath.Join(f.root, "起点"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	s := f.connect(t, func(c *Config) { c.Root = "起点" })
	ctx := context.Background()

	put(t, ctx, s, "/中身.txt", "なかみ")

	if _, err := os.Stat(filepath.Join(f.root, "起点", "中身.txt")); err != nil {
		t.Errorf("起点の下にない: %v", err)
	}
	if got := readAll(t, ctx, s, "/中身.txt"); got != "なかみ" {
		t.Errorf("内容 = %q", got)
	}
}

// 複数の接続を使って並行に転送できることを確かめます。
//
// FTP は1つの接続で1つのやりとりしかできないので、
// 使い回せる接続をまとめて持っています。
func TestConcurrentTransfers(t *testing.T) {
	f := startFakeFTP(t)
	s := f.connect(t, func(c *Config) { c.MaxConns = 4 })
	ctx := context.Background()

	done := make(chan error, 8)
	for i := range 8 {
		go func() {
			_, err := s.Put(ctx,
				"/並行/"+string(rune('a'+i))+".txt",
				strings.NewReader("なかみ"),
				storage.ObjectMeta{Size: 9})
			done <- err
		}()
	}
	for range 8 {
		if err := <-done; err != nil {
			t.Errorf("並行の書き込み: %v", err)
		}
	}

	count := 0
	if err := s.List(ctx, "/並行", func(storage.FileInfo) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if count != 8 {
		t.Errorf("書き込めた件数 = %d, want 8", count)
	}
}

// ログインに失敗したら、その場ではっきり分かることを確かめます。
//
// 以前の実装はログインの結果を確かめておらず、ログインできていない
// まま次の操作に進んでいました。
func TestLoginFailure(t *testing.T) {
	f := startFakeFTP(t)

	host, port, _ := strings.Cut(f.addr, ":")
	_, err := New(context.Background(), Config{
		Name:     "合言葉ちがい",
		Host:     host,
		Port:     mustAtoi(t, port),
		User:     testUser,
		Password: "ちがう",
		TLS:      TLSNone,
	})
	if err == nil {
		t.Fatal("合言葉が違うのに繋がってしまった")
	}
	if !strings.Contains(err.Error(), "ログインに失敗") {
		t.Errorf("何が起きたのか分からない: %v", err)
	}
}

func TestClassifyCode(t *testing.T) {
	tests := []struct {
		code     int
		sentinel error
		class    storage.Class
	}{
		{550, storage.ErrNotFound, storage.ClassPermanent},
		{450, storage.ErrNotFound, storage.ClassPermanent},
		{530, nil, storage.ClassAuth},
		{421, nil, storage.ClassRetryable},
		{426, nil, storage.ClassRetryable},
		// 容量が足りないのは待っても直らない。
		{452, nil, storage.ClassPermanent},
		{552, nil, storage.ClassPermanent},
		{502, storage.ErrUnsupported, storage.ClassPermanent},
	}
	for _, tt := range tests {
		got := classifyCode(tt.code)
		if !errors.Is(got.sentinel, tt.sentinel) {
			t.Errorf("classifyCode(%d) の番兵 = %v, want %v", tt.code, got.sentinel, tt.sentinel)
		}
		if got.class != tt.class {
			t.Errorf("classifyCode(%d) の種類 = %v, want %v", tt.code, got.class, tt.class)
		}
	}
}

func TestLeadingCode(t *testing.T) {
	tests := []struct {
		in   string
		code int
		ok   bool
	}{
		{"550 Not found", 550, true},
		{"421", 421, true},
		{"接続できません", 0, false},
		{"ab", 0, false},
	}
	for _, tt := range tests {
		code, ok := leadingCode(tt.in)
		if ok != tt.ok || (ok && code != tt.code) {
			t.Errorf("leadingCode(%q) = %d, %v, want %d, %v", tt.in, code, ok, tt.code, tt.ok)
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
		{"接続先がない", Config{}, "host"},
		{"知らない暗号化の指定", Config{Host: "h", TLS: "たぶん"}, "tls"},
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

// 既定では暗号化することを確かめます。
//
// FTP は合言葉も中身も平文で流れるので、既定で AUTH TLS を試みます。
func TestTLSDefaultsToExplicit(t *testing.T) {
	if got := (Config{}).tls(); got != TLSExplicit {
		t.Errorf("既定の暗号化 = %q, want %q", got, TLSExplicit)
	}
}

// 名前を省略したら匿名での接続になることを確かめます。
func TestAnonymousDefaults(t *testing.T) {
	cfg := Config{}
	if got := cfg.user(); got != "anonymous" {
		t.Errorf("既定のログイン名 = %q, want anonymous", got)
	}
	if cfg.password() == "" {
		t.Error("匿名での接続に連絡先が入っていない")
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
