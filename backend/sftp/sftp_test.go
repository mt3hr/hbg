package sftp

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
	"golang.org/x/crypto/ssh"
)

// 適合性テストを試験用の SSH サーバーに対して実行します。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			srv := startFakeServer(t, t.TempDir())
			s, _ := srv.connect(t)

			root := "試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		// 試験用のサーバーは pkg/sftp のサーバー実装で、要求されたパスを
		// そのままこの計算機のファイルシステムに向けます。Windows では
		// 名前の中の "\\" が区切りとして解釈されてしまうため、この試験では
		// 使えません。実際の SFTP サーバーは POSIX なので問題ありません。
		IllegalNameChars: windowsOnlyIllegalChars(),
	})
}

// windowsOnlyIllegalChars は、Windows の試験でだけ避ける文字を返します。
func windowsOnlyIllegalChars() string {
	if runtime.GOOS == "windows" {
		return `\`
	}
	return ""
}

func newTestStorage(t *testing.T) (context.Context, string, *Storage) {
	t.Helper()
	dir := t.TempDir()
	srv := startFakeServer(t, dir)
	s, _ := srv.connect(t)
	return context.Background(), dir, s
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
//
// 別名で書いてから置き換えるので、途中で失敗しても
// 中身の欠けたファイルが本来の場所に残りません。
func TestPutIsAtomic(t *testing.T) {
	ctx, dir, s := newTestStorage(t)

	// 途中で失敗する読み手。
	broken := io.MultiReader(
		strings.NewReader(strings.Repeat("x", 100)),
		errReader{errors.New("読み取りに失敗しました")},
	)

	_, err := s.Put(ctx, "壊れる.txt", broken, storage.ObjectMeta{Size: 1000})
	if err == nil {
		t.Fatal("失敗するはずの書き込みが成功した")
	}

	if _, err := os.Stat(filepath.Join(dir, "壊れる.txt")); !os.IsNotExist(err) {
		t.Error("中身の欠けたファイルが残っている")
	}

	// 書きかけの一時ファイルも残らないこと。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("後始末されていないファイルが残っている: %s", e.Name())
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// 書き込み中の一時ファイルが一覧に出ないことを確かめます。
func TestPartFilesAreHidden(t *testing.T) {
	ctx, dir, s := newTestStorage(t)

	// 前回の中断で残ったものを模す。
	if err := os.WriteFile(filepath.Join(dir, ".のこり.txt"+partSuffix), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	put(t, ctx, s, "ふつう.txt", "なかみ")

	names := []string{}
	if err := s.List(ctx, ".", func(fi storage.FileInfo) error {
		names = append(names, fi.Name)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(names) != 1 || names[0] != "ふつう.txt" {
		t.Errorf("一覧 = %v, want [ふつう.txt]", names)
	}
}

// 更新時刻が保たれることを確かめます。
func TestModTimeRoundTrip(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	want := time.Date(2021, 6, 15, 12, 34, 56, 0, time.UTC)
	if _, err := s.Put(ctx, "時刻.txt", strings.NewReader("x"), storage.ObjectMeta{
		Size:    1,
		ModTime: want,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	fi, err := s.Stat(ctx, "時刻.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// SFTP の時刻は秒まで。
	if diff := fi.ModTime.Sub(want); diff > time.Second || diff < -time.Second {
		t.Errorf("更新時刻 = %v, want %v", fi.ModTime, want)
	}

	// あとから変えられること。
	later := want.Add(48 * time.Hour)
	if err := s.SetModTime(ctx, "時刻.txt", later); err != nil {
		t.Fatalf("SetModTime: %v", err)
	}
	fi, err = s.Stat(ctx, "時刻.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if diff := fi.ModTime.Sub(later); diff > time.Second || diff < -time.Second {
		t.Errorf("変更後の更新時刻 = %v, want %v", fi.ModTime, later)
	}
}

// Remove が中身ごと消してしまわないことを確かめます。
func TestRemoveRefusesNonEmptyDir(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "消さない/中身.txt", "だいじ")

	err := s.Remove(ctx, "消さない")
	if !errors.Is(err, storage.ErrNotEmpty) {
		t.Fatalf("Remove = %v, want ErrNotEmpty", err)
	}
	if _, err := s.Stat(ctx, "消さない/中身.txt"); err != nil {
		t.Errorf("中身が消えている: %v", err)
	}

	if err := s.Purge(ctx, "消さない"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, err := s.Stat(ctx, "消さない"); !storage.IsNotFound(err) {
		t.Errorf("Purge のあとも残っている: %v", err)
	}
}

// root を指定すると、その下が起点になることを確かめます。
func TestRootIsApplied(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "起点"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	srv := startFakeServer(t, dir)
	s, _ := srv.connect(t, func(c *Config) { c.Root = "起点" })
	ctx := context.Background()

	put(t, ctx, s, "中身.txt", "なかみ")

	// 起点の下に書かれていること。
	if _, err := os.Stat(filepath.Join(dir, "起点", "中身.txt")); err != nil {
		t.Errorf("起点の下にない: %v", err)
	}
	if got := readAll(t, ctx, s, "中身.txt"); got != "なかみ" {
		t.Errorf("内容 = %q", got)
	}
}

// はじめてのホストの鍵を記録することを確かめます。
func TestAcceptNewRecordsHostKey(t *testing.T) {
	srv := startFakeServer(t, t.TempDir())

	knownHosts := filepath.Join(t.TempDir(), "known_hosts")
	_, notices := srv.connect(t, func(c *Config) {
		c.KnownHostsFile = knownHosts
		c.StrictHostKeyChecking = StrictAcceptNew
	})

	data, err := os.ReadFile(knownHosts)
	if err != nil {
		t.Fatalf("記録を読めません: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("ホスト鍵が記録されていない")
	}

	// 利用者に知らせること。黙って信用しない。
	if len(notices) == 0 {
		t.Error("ホスト鍵を記録したことが知らされていない")
	}
	fingerprint := ssh.FingerprintSHA256(srv.hostKey)
	if !strings.Contains(strings.Join(notices, "\n"), fingerprint) {
		t.Errorf("通知に指紋が含まれていない: %v", notices)
	}

	// 2度目は記録が増えないこと。
	if _, err := srv.connect(t, func(c *Config) {
		c.KnownHostsFile = knownHosts
		c.StrictHostKeyChecking = StrictAcceptNew
	}); err == nil {
		// connect は失敗したら t.Fatal するので、ここに来れば成功。
		after, readErr := os.ReadFile(knownHosts)
		if readErr != nil {
			t.Fatalf("記録を読めません: %v", readErr)
		}
		if len(after) != len(data) {
			t.Error("2度目の接続で記録が増えている")
		}
	}
}

// 記録にないホストへは繋がないことを確かめます。
func TestStrictYesRejectsUnknownHost(t *testing.T) {
	srv := startFakeServer(t, t.TempDir())

	host, port, _ := splitHostPort(t, srv.addr)
	_, err := New(context.Background(), Config{
		Name:                  "きびしい",
		Host:                  host,
		Port:                  port,
		User:                  testUser,
		Password:              testPassword,
		KnownHostsFile:        filepath.Join(t.TempDir(), "known_hosts"),
		StrictHostKeyChecking: StrictYes,
	})
	if err == nil {
		t.Fatal("記録にないホストに繋がってしまった")
	}
	if !strings.Contains(err.Error(), "accept-new") {
		t.Errorf("どうすればよいか分からない: %v", err)
	}
}

// ホスト鍵が変わっていたら、accept-new でも拒否することを確かめます。
//
// はじめてのホストを受け入れるのと、鍵が変わったのを受け入れるのは
// まったく別の話です。後者は中間者攻撃の兆候でもあります。
func TestAcceptNewRejectsChangedHostKey(t *testing.T) {
	knownHosts := filepath.Join(t.TempDir(), "known_hosts")

	first := startFakeServer(t, t.TempDir())
	first.connect(t, func(c *Config) {
		c.KnownHostsFile = knownHosts
		c.StrictHostKeyChecking = StrictAcceptNew
	})
	_ = first.listener.Close()

	// 同じ待ち受け先に、別の鍵のサーバーを立て直すことはできないので、
	// 記録のほうのホスト名を second の待ち受け先に書き換える。
	second := startFakeServer(t, t.TempDir())
	rewriteKnownHostsAddr(t, knownHosts, first.addr, second.addr)

	host, port, _ := splitHostPort(t, second.addr)
	_, err := New(context.Background(), Config{
		Name:                  "鍵が変わった",
		Host:                  host,
		Port:                  port,
		User:                  testUser,
		Password:              testPassword,
		KnownHostsFile:        knownHosts,
		StrictHostKeyChecking: StrictAcceptNew,
	})
	if err == nil {
		t.Fatal("鍵が変わっているのに繋がってしまった")
	}
	if !strings.Contains(err.Error(), "ホスト鍵が記録と違います") {
		t.Errorf("何が起きたのか分からない: %v", err)
	}
}

// rewriteKnownHostsAddr は記録の中の待ち受け先を書き換えます。
func rewriteKnownHostsAddr(t *testing.T, file, from, to string) {
	t.Helper()

	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("記録を読めません: %v", err)
	}

	// knownhosts は既定でないポートを [host]:port の形で書く。
	replaced := strings.ReplaceAll(string(data), bracketed(from), bracketed(to))
	if replaced == string(data) {
		t.Fatalf("記録の中に %s が見つかりません:\n%s", bracketed(from), data)
	}
	if err := os.WriteFile(file, []byte(replaced), 0o600); err != nil {
		t.Fatalf("記録を書けません: %v", err)
	}
}

func bracketed(addr string) string {
	host, port, found := strings.Cut(addr, ":")
	if !found {
		return addr
	}
	return "[" + host + "]:" + port
}

func splitHostPort(t *testing.T, addr string) (string, int, error) {
	t.Helper()
	host, port, found := strings.Cut(addr, ":")
	if !found {
		t.Fatalf("待ち受け先を解釈できません: %q", addr)
	}
	return host, mustAtoi(t, port), nil
}

func TestCleanPath(t *testing.T) {
	tests := map[string]string{
		"":          ".",
		".":         ".",
		"/":         "/",
		"a/b":       "a/b",
		"/a/b":      "/a/b",
		"/a//b":     "/a/b",
		"/a/./b":    "/a/b",
		"/a/b/../c": "/a/c",
		// 逆斜線は区切りではなく、名前の一部として扱う。
		"a\\b": "a\\b",
	}
	for in, want := range tests {
		if got := cleanPath(in); got != want {
			t.Errorf("cleanPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJoinRoot(t *testing.T) {
	tests := []struct {
		root, p, want string
	}{
		{"", "a/b", "a/b"},
		{"/起点", "a/b", "/起点/a/b"},
		{"/起点", "", "/起点"},
		// 起点を指定していても、絶対パスはそのまま使う。
		{"/起点", "/別の場所/x", "/別の場所/x"},
	}
	for _, tt := range tests {
		if got := joinRoot(tt.root, tt.p); got != tt.want {
			t.Errorf("joinRoot(%q, %q) = %q, want %q", tt.root, tt.p, got, tt.want)
		}
	}
}

func TestTempPath(t *testing.T) {
	tests := map[string]string{
		"/a/b.txt": "/a/.b.txt" + partSuffix,
		"b.txt":    ".b.txt" + partSuffix,
	}
	for in, want := range tests {
		if got := tempPath(in); got != want {
			t.Errorf("tempPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		code uint32
		want storage.Class
	}{
		{2, storage.ClassPermanent}, // SSH_FX_NO_SUCH_FILE
		{3, storage.ClassPermanent}, // SSH_FX_PERMISSION_DENIED
		{6, storage.ClassRetryable}, // SSH_FX_NO_CONNECTION
		{7, storage.ClassRetryable}, // SSH_FX_CONNECTION_LOST
		{8, storage.ClassPermanent}, // SSH_FX_OP_UNSUPPORTED
		{4, storage.ClassUnknown},   // SSH_FX_FAILURE は何にでも使われる
	}
	for _, tt := range tests {
		if got := classifyStatus(tt.code); got.class != tt.want {
			t.Errorf("classifyStatus(%d) = %v, want %v", tt.code, got.class, tt.want)
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
		{"接続先がない", Config{User: "u"}, "host"},
		{"ログイン名がない", Config{Host: "h"}, "user"},
		{"知らない確かめ方", Config{Host: "h", User: "u", StrictHostKeyChecking: "maybe"}, "strict_host_key_checking"},
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

// 大きめのファイルでも壊れずに往復することを確かめます。
func TestLargerFile(t *testing.T) {
	if testing.Short() {
		t.Skip("-short のため飛ばします")
	}
	ctx, _, s := newTestStorage(t)

	content := strings.Repeat("この行は繰り返されます。\n", 20000)
	put(t, ctx, s, path.Join("大きい", "file.txt"), content)

	if got := readAll(t, ctx, s, "大きい/file.txt"); got != content {
		t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
	}
}
