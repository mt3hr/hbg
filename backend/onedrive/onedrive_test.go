package onedrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/internal/auth"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 適合性テストを偽サーバーに対して実行します。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			f := newFakeGraph()
			s := f.start(t)

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		LargeDirCount: 60,
	})
}

func newTestStorage(t *testing.T, mutate ...func(*Config)) (context.Context, *fakeGraph, *Storage) {
	t.Helper()
	f := newFakeGraph()
	return context.Background(), f, f.start(t, mutate...)
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

// 元のファイルの更新時刻が保たれることを確かめます。
//
// Graph が返す lastModifiedDateTime は「サーバー上で変わった時刻」で、
// 元のファイルの更新時刻ではありません。同期の判断に要るのは
// fileSystemInfo のほうです。
func TestModTimeUsesFileSystemInfo(t *testing.T) {
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

	// 一覧でも同じ時刻が見えること。
	var listed storage.FileInfo
	if err := s.List(ctx, "/", func(e storage.FileInfo) error {
		if e.Name == "時刻.txt" {
			listed = e
		}
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if !listed.ModTime.Equal(want) {
		t.Errorf("一覧の更新時刻 = %v, want %v", listed.ModTime, want)
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
	if !fi.ModTime.Equal(later) {
		t.Errorf("変更後の更新時刻 = %v, want %v", fi.ModTime, later)
	}
}

// 分割送信の経路を通り、内容が壊れないことを確かめます。
//
// Graph の分割送信は1つぶんの大きさが 320KiB の倍数でなければ
// 受け付けられません。偽サーバー側でもそれを確かめています。
func TestChunkedUpload(t *testing.T) {
	origSmall, origChunk := smallUploadLimit, defaultChunkSize
	smallUploadLimit = chunkUnit
	defaultChunkSize = chunkUnit
	t.Cleanup(func() { smallUploadLimit, defaultChunkSize = origSmall, origChunk })

	sizes := []int{
		chunkUnit - 1,
		chunkUnit,
		chunkUnit + 1,
		chunkUnit * 2,
		chunkUnit*2 + 12345,
	}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%dバイト", size), func(t *testing.T) {
			ctx, f, s := newTestStorage(t)

			content := strings.Repeat("a", size)
			put(t, ctx, s, "/大きい.bin", content)

			if got := readAll(t, ctx, s, "/大きい.bin"); got != content {
				t.Errorf("内容の長さ = %d, want %d", len(got), size)
			}

			// 境界を超えたものだけ分割送信を使うこと。
			sessions := f.callCount("create_session")
			wantSession := size > chunkUnit
			if (sessions > 0) != wantSession {
				t.Errorf("分割送信の利用 = %v, want %v（%dバイト）", sessions > 0, wantSession, size)
			}
		})
	}
}

// 分割送信でも更新時刻が保たれることを確かめます。
func TestChunkedUploadKeepsModTime(t *testing.T) {
	origSmall, origChunk := smallUploadLimit, defaultChunkSize
	smallUploadLimit = chunkUnit
	defaultChunkSize = chunkUnit
	t.Cleanup(func() { smallUploadLimit, defaultChunkSize = origSmall, origChunk })

	ctx, _, s := newTestStorage(t)

	want := time.Date(2020, 3, 1, 9, 0, 0, 0, time.UTC)
	content := strings.Repeat("b", chunkUnit*2+100)
	if _, err := s.Put(ctx, "/大きい.bin", strings.NewReader(content), storage.ObjectMeta{
		Size:    int64(len(content)),
		ModTime: want,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	fi, err := s.Stat(ctx, "/大きい.bin")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !fi.ModTime.Equal(want) {
		t.Errorf("更新時刻 = %v, want %v", fi.ModTime, want)
	}
}

// サイズが実際より小さく伝えられても、内容が切り詰められないことを確かめます。
func TestPutIgnoresDeclaredSize(t *testing.T) {
	origSmall, origChunk := smallUploadLimit, defaultChunkSize
	smallUploadLimit = chunkUnit
	defaultChunkSize = chunkUnit
	t.Cleanup(func() { smallUploadLimit, defaultChunkSize = origSmall, origChunk })

	ctx, _, s := newTestStorage(t)

	content := strings.Repeat("c", chunkUnit*2+7)
	for _, declared := range []int64{0, storage.SizeUnknown, 3} {
		t.Run(fmt.Sprintf("宣言サイズ %d", declared), func(t *testing.T) {
			p := fmt.Sprintf("/宣言%d.bin", declared)

			written, err := s.Put(ctx, p, strings.NewReader(content), storage.ObjectMeta{
				Size: declared,
			})
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if written.Size != int64(len(content)) {
				t.Errorf("書き込まれたサイズ = %d, want %d", written.Size, len(content))
			}
			if got := readAll(t, ctx, s, p); got != content {
				t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
			}
		})
	}
}

// 分割送信の送り先に認証の情報を付けないことを確かめます。
//
// 送り先は署名済みの一時的な接続先で、認証の情報を付けると
// 拒否されることがあります。偽サーバー側で確かめています。
func TestUploadSessionHasNoAuthHeader(t *testing.T) {
	origSmall, origChunk := smallUploadLimit, defaultChunkSize
	smallUploadLimit = chunkUnit
	defaultChunkSize = chunkUnit
	t.Cleanup(func() { smallUploadLimit, defaultChunkSize = origSmall, origChunk })

	f := newFakeGraph()
	s := f.start(t, func(c *Config) {
		// 認証を付ける層を模す。
		c.httpOverride = &http.Client{Transport: &authAddingTransport{}}
	})
	ctx := context.Background()

	content := strings.Repeat("d", chunkUnit*2)
	put(t, ctx, s, "/大きい.bin", content)

	if got := readAll(t, ctx, s, "/大きい.bin"); got != content {
		t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
	}
}

// authAddingTransport は認証の情報を付ける層です。
// hbg の noAuthTransport と同じ組み合わせを作ります。
type authAddingTransport struct{}

func (t *authAddingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	inner := &noAuthTransport{inner: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer 偽のトークン")
		return http.DefaultTransport.RoundTrip(r)
	})}
	return inner.RoundTrip(req)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// 続きの取得が漏れないことを確かめます。
func TestListFollowsNextLink(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	const n = 20
	for i := range n {
		put(t, ctx, s, fmt.Sprintf("/多い/%03d.txt", i), "x")
	}

	count := 0
	if err := s.List(ctx, "/多い", func(storage.FileInfo) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if count != n {
		t.Errorf("列挙できた件数 = %d, want %d", count, n)
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
	ctx, f, s := newTestStorage(t, func(c *Config) { c.Root = "起点" })

	put(t, ctx, s, "/中身.txt", "なかみ")

	f.mu.Lock()
	_, ok := f.items["起点/中身.txt"]
	f.mu.Unlock()
	if !ok {
		t.Error("起点の下に書かれていない")
	}
	if got := readAll(t, ctx, s, "/中身.txt"); got != "なかみ" {
		t.Errorf("内容 = %q", got)
	}
}

// 待つよう指示された時間が伝わることを確かめます。
func TestRetryAfterIsHonored(t *testing.T) {
	ctx, f, s := newTestStorage(t)
	f.failNextWithRetryAfter("get", 100, http.StatusTooManyRequests, "activityLimitReached", "42")

	_, err := s.Stat(ctx, "/どこか.txt")
	if err == nil {
		t.Fatal("成功してしまった")
	}
	if class := storage.ClassOf(err); class != storage.ClassRateLimit {
		t.Errorf("失敗の種類 = %v, want ratelimit", class)
	}
	if got := storage.RetryAfterOf(err); got != 42*time.Second {
		t.Errorf("待ち時間 = %v, want 42s（サーバーの指示に従うこと）", got)
	}
}

// 認証の失敗は再試行せず、処理全体を止めることを確かめます。
func TestAuthErrorIsFatal(t *testing.T) {
	ctx, f, s := newTestStorage(t)
	f.failNext("get", 100, http.StatusUnauthorized, "invalidAuthenticationToken")

	_, err := s.Stat(ctx, "/どこか.txt")
	if class := storage.ClassOf(err); class != storage.ClassAuth {
		t.Errorf("失敗の種類 = %v, want auth", class)
	}
	if storage.ClassOf(err).Retryable() {
		t.Error("認証の失敗が再試行の対象になっている")
	}
}

func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		status   int
		code     string
		sentinel error
		class    storage.Class
	}{
		{404, "itemNotFound", storage.ErrNotFound, storage.ClassPermanent},
		{401, "invalidAuthenticationToken", nil, storage.ClassAuth},
		{403, "accessDenied", nil, storage.ClassAuth},
		// 容量が足りないのは待っても直らない。
		{403, "quotaLimitReached", nil, storage.ClassPermanent},
		{429, "activityLimitReached", nil, storage.ClassRateLimit},
		{409, "nameAlreadyExists", storage.ErrExist, storage.ClassPermanent},
		{503, "serviceNotAvailable", nil, storage.ClassRetryable},
		{500, "", nil, storage.ClassRetryable},
		{400, "invalidRequest", nil, storage.ClassPermanent},
	}
	for _, tt := range tests {
		got := classifyStatus(tt.status, tt.code)
		if !errors.Is(got.sentinel, tt.sentinel) {
			t.Errorf("classifyStatus(%d, %q) の番兵 = %v, want %v",
				tt.status, tt.code, got.sentinel, tt.sentinel)
		}
		if got.class != tt.class {
			t.Errorf("classifyStatus(%d, %q) の種類 = %v, want %v",
				tt.status, tt.code, got.class, tt.class)
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
		{"知らないドライブの種類", Config{DriveType: "どこか"}, "drive_type"},
		{"sharepoint なのに宛先がない", Config{DriveType: DriveTypeSharePoint}, "site_id"},
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

// ドライブの入口が種類によって変わることを確かめます。
func TestDriveRoot(t *testing.T) {
	tests := []struct {
		cfg  Config
		want string
	}{
		{Config{}, "/me/drive"},
		{Config{DriveType: DriveTypeBusiness}, "/me/drive"},
		{Config{DriveID: "DRIVE"}, "/drives/DRIVE"},
		{Config{DriveType: DriveTypeSharePoint, SiteID: "SITE"}, "/sites/SITE/drive"},
	}
	for _, tt := range tests {
		if got := tt.cfg.driveRoot(); got != tt.want {
			t.Errorf("driveRoot(%+v) = %q, want %q", tt.cfg, got, tt.want)
		}
	}
}

// パスの組み立てを確かめます。
func TestItemURL(t *testing.T) {
	c := &graphClient{base: "https://例.invalid/v1.0", driveRoot: "/me/drive"}

	tests := []struct {
		p, suffix, want string
	}{
		{"", "", "https://例.invalid/v1.0/me/drive/root"},
		{"", "children", "https://例.invalid/v1.0/me/drive/root/children"},
		{"写真", "", "https://例.invalid/v1.0/me/drive/root:/%E5%86%99%E7%9C%9F"},
		{"写真/a.jpg", "content", "https://例.invalid/v1.0/me/drive/root:/%E5%86%99%E7%9C%9F/a.jpg:/content"},
		// 記号を含む名前も、そのまま埋め込まずに符号化する。
		{"a b#c", "", "https://例.invalid/v1.0/me/drive/root:/a%20b%23c"},
	}
	for _, tt := range tests {
		if got := c.itemURL(tt.p, tt.suffix); got != tt.want {
			t.Errorf("itemURL(%q, %q) = %q, want %q", tt.p, tt.suffix, got, tt.want)
		}
	}
}

func TestCleanPath(t *testing.T) {
	tests := map[string]string{
		"":          "/",
		"/":         "/",
		"a/b":       "/a/b",
		"/a//b":     "/a/b",
		"/a/b/../c": "/a/c",
	}
	for in, want := range tests {
		if got := cleanPath(in); got != want {
			t.Errorf("cleanPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// リダイレクト URI がアプリ登録時の案内と一致していることを確かめます。
// Dropbox と同じ理由です（外れると認可画面でしか分からない）。
func TestLoginFlowRedirectMatchesRegisteredURIs(t *testing.T) {
	flow := loginFlow(auth.MicrosoftOAuth2Config(auth.ClientCredentials{ClientID: "id"}, ""), auth.LoginOptions{})

	if flow.RedirectHost != auth.MicrosoftRedirectHost {
		t.Errorf("RedirectHost = %q, want %q", flow.RedirectHost, auth.MicrosoftRedirectHost)
	}
	if len(flow.FixedPorts) == 0 {
		t.Fatal("ポートを固定していない")
	}
	registered := auth.MicrosoftRedirectURIs()
	for _, port := range flow.FixedPorts {
		uri := auth.RedirectURI(flow.RedirectHost, port)
		if !slices.Contains(registered, uri) {
			t.Errorf("%s を使いうるが、登録案内に載っていない: %v", uri, registered)
		}
	}
	if !flow.UsePKCE {
		t.Error("PKCE を使っていない。パブリッククライアントでは必須")
	}
}
