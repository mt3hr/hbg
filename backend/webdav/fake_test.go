package webdav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

// 試験用の WebDAV サーバーです。
//
// golang.org/x/net/webdav に本物のサーバー実装があるので、
// それをこの計算機のディレクトリに向けます。手続き（PROPFIND や
// MKCOL や MOVE）は実物と同じものが流れるので、hbg 側の組み立てを
// 実際のやりとりを通して確かめられます。
//
// Nextcloud 独自の X-OC-Mtime は実装されていないので、こちらで足します。

const (
	testUser     = "試験利用者"
	testPassword = "ひみつ"
)

// fakeWebDAV は試験用のサーバーです。
type fakeWebDAV struct {
	root    string
	handler *webdav.Handler

	mu sync.Mutex
	// calls は手続きごとの呼び出し回数です。
	calls map[string]int
	// failures は手続きごとの「あと何回失敗させるか」です。
	failures map[string]*fakeFailure
	// mtimeEnabled が真なら X-OC-Mtime を受け付けます。
	mtimeEnabled bool
}

type fakeFailure struct {
	remaining int
	status    int
}

func newFakeWebDAV(root string) *fakeWebDAV {
	return &fakeWebDAV{
		root: root,
		handler: &webdav.Handler{
			FileSystem: webdav.Dir(root),
			LockSystem: webdav.NewMemLS(),
		},
		calls:    map[string]int{},
		failures: map[string]*fakeFailure{},
	}
}

func (f *fakeWebDAV) failNext(method string, n, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[method] = &fakeFailure{remaining: n, status: status}
}

func (f *fakeWebDAV) callCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

func (f *fakeWebDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.calls[r.Method]++
	if fail, ok := f.failures[r.Method]; ok && fail.remaining > 0 {
		fail.remaining--
		status := fail.status
		f.mu.Unlock()
		w.WriteHeader(status)
		return
	}
	mtimeEnabled := f.mtimeEnabled
	f.mu.Unlock()

	// ごく素朴な認証。合言葉が違えば断る。
	if user, pass, ok := r.BasicAuth(); !ok || user != testUser || pass != testPassword {
		w.Header().Set("WWW-Authenticate", `Basic realm="試験"`)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// X-OC-Mtime は Nextcloud / ownCloud の独自のヘッダで、
	// x/net/webdav には実装がない。書き込んだあとに時刻を合わせる。
	mtime := time.Time{}
	if raw := r.Header.Get(mtimeHeader); mtimeEnabled && raw != "" && r.Method == http.MethodPut {
		if sec, err := strconv.ParseInt(raw, 10, 64); err == nil {
			mtime = time.Unix(sec, 0)
		}
	}

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	f.handler.ServeHTTP(rec, r)

	if !mtime.IsZero() && rec.status < 300 {
		local := f.localPath(r.URL.Path)
		_ = os.Chtimes(local, mtime, mtime)
	}
}

// localPath は要求のパスをこの計算機のパスに直します。
func (f *fakeWebDAV) localPath(p string) string {
	return f.root + strings.TrimSuffix(p, "/")
}

// statusRecorder は応答の状態コードを控えます。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// start は試験用のサーバーを立ち上げ、そこへ向いたストレージを返します。
func (f *fakeWebDAV) start(t *testing.T, mutate ...func(*Config)) *Storage {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	cfg := Config{
		Name:     "偽webdav",
		URL:      srv.URL,
		User:     testUser,
		Password: testPassword,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	if cfg.canSetModTime() {
		f.mu.Lock()
		f.mtimeEnabled = true
		f.mu.Unlock()
	}

	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("ストレージを作れません: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// newTestStorage は試験用のストレージを作ります。
func newTestStorage(t *testing.T, mutate ...func(*Config)) (context.Context, *fakeWebDAV, *Storage) {
	t.Helper()
	f := newFakeWebDAV(t.TempDir())
	return context.Background(), f, f.start(t, mutate...)
}
