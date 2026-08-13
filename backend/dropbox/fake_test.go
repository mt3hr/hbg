package dropbox

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dbxapi "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/retry"
	"github.com/mt3hr/hbg/storage"
)

// 偽の Dropbox サーバーです。
//
// 認証情報なしで、ページ分割・チャンク分割・エラーの分類・再試行までを
// 試験できるようにするためのものです。SDK は Config.URLGenerator を
// 「試験用」として公開しているので、そこを差し替えて到達先を変えます。
//
// 実物と意図的に振る舞いを変えている点は fakeDropbox の各所に記しています。

// fakeEntry は偽サーバー上の1件です。
type fakeEntry struct {
	// path は表示用のパスです（大文字小文字は登録時のまま）。
	path  string
	isDir bool
	data  []byte
	// modified は client_modified です。
	modified time.Time
	id       string
}

// fakeSession はアップロードセッションです。
type fakeSession struct {
	data []byte
}

// fakeCursor は List の続きです。
type fakeCursor struct {
	entries []*fakeEntry
	offset  int
}

// fakeDropbox は Dropbox API のごく一部を再現します。
type fakeDropbox struct {
	mu sync.Mutex
	// entries は小文字にしたパスをキーにした一覧です。
	// Dropbox は大文字小文字を区別しないため、実物に合わせます。
	entries  map[string]*fakeEntry
	sessions map[string]*fakeSession
	cursors  map[string]*fakeCursor
	seq      int

	// pageSize は List が1回に返す件数です。
	// 小さくしてあるので、どんなに小さいディレクトリでも
	// 続きの取得を必ず通ります。
	pageSize int

	// failures は経路ごとの「あと何回失敗させるか」です。
	// 再試行の試験に使います。
	failures map[string]*fakeFailure

	// calls は経路ごとの呼び出し回数です。
	calls map[string]int
}

// fakeFailure は注入する失敗です。
type fakeFailure struct {
	remaining int
	status    int
	body      string
}

func newFakeDropbox() *fakeDropbox {
	return &fakeDropbox{
		entries:  map[string]*fakeEntry{},
		sessions: map[string]*fakeSession{},
		cursors:  map[string]*fakeCursor{},
		pageSize: 3,
		failures: map[string]*fakeFailure{},
		calls:    map[string]int{},
	}
}

// start は偽サーバーを立ち上げ、そこへ向いたストレージを返します。
func (f *fakeDropbox) start(t *testing.T) *Storage {
	return f.startWith(t, retryPolicy())
}

// startNoRetry は再試行しないストレージを返します。
// 失敗の分類そのものを確かめたいとき、待ち時間を挟まずに済みます。
func (f *fakeDropbox) startNoRetry(t *testing.T) *Storage {
	return f.startWith(t, nil)
}

func (f *fakeDropbox) startWith(t *testing.T, policy *retry.Policy) *Storage {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	client := dbx.NewContext(dbxapi.Config{
		Token: "偽のトークン",
		// 経路の名前をそのままパスにする。実物の api./content. の
		// 使い分けは、ここでは区別する必要がない。
		URLGenerator: func(_, namespace, route string) string {
			return fmt.Sprintf("%s/2/%s/%s", srv.URL, namespace, route)
		},
		RetryPolicy: policy,
	})
	return newWithClient("偽dropbox", client)
}

// failNext は次の n 回、その経路を失敗させます。
func (f *fakeDropbox) failNext(route string, n, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[route] = &fakeFailure{remaining: n, status: status, body: body}
}

// callCount はその経路が呼ばれた回数を返します。
func (f *fakeDropbox) callCount(route string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[route]
}

func (f *fakeDropbox) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := strings.TrimPrefix(r.URL.Path, "/2/files/")

	f.mu.Lock()
	f.calls[route]++
	if fail, ok := f.failures[route]; ok && fail.remaining > 0 {
		fail.remaining--
		status, body := fail.status, fail.body
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
		return
	}
	f.mu.Unlock()

	switch route {
	case "get_metadata":
		f.handle(w, r, f.getMetadata)
	case "list_folder":
		f.handle(w, r, f.listFolder)
	case "list_folder/continue":
		f.handle(w, r, f.listFolderContinue)
	case "create_folder_v2":
		f.handle(w, r, f.createFolder)
	case "delete_v2":
		f.handle(w, r, f.delete)
	case "copy_v2":
		f.handle(w, r, f.relocate(false))
	case "move_v2":
		f.handle(w, r, f.relocate(true))
	case "download":
		f.download(w, r)
	case "upload":
		f.upload(w, r)
	case "upload_session/start":
		f.sessionStart(w, r)
	case "upload_session/append_v2":
		f.sessionAppend(w, r)
	case "upload_session/finish":
		f.sessionFinish(w, r)
	default:
		http.Error(w, "知らない経路です: "+route, http.StatusNotFound)
	}
}

// apiError は Dropbox の 409 応答です。
type apiError struct {
	summary string
}

func (e apiError) Error() string { return e.summary }

func errNotFound() error  { return apiError{"path/not_found/."} }
func errNotFolder() error { return apiError{"path/not_folder/."} }
func errNotFile() error   { return apiError{"path/not_file/."} }

func conflictErr(kind string) error { return apiError{"path/conflict/" + kind + "/."} }
func toConflictErr() error          { return apiError{"to/conflict/file/."} }

// handle は JSON をやりとりする経路の共通処理です。
func (f *fakeDropbox) handle(w http.ResponseWriter, r *http.Request, fn func(json.RawMessage) (any, error)) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	res, err := fn(body)
	f.mu.Unlock()

	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, res)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, err error) {
	var ae apiError
	if !asAPIError(err, &ae) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error_summary": ae.summary,
		"error":         map[string]any{".tag": "path"},
	})
}

func asAPIError(err error, out *apiError) bool {
	return errors.As(err, out)
}

// --- 蓄えの操作（呼び出し側で mu を保持していること） ---

func key(p string) string { return strings.ToLower(p) }

func (f *fakeDropbox) get(p string) *fakeEntry {
	if p == "" {
		return &fakeEntry{path: "", isDir: true}
	}
	return f.entries[key(p)]
}

func (f *fakeDropbox) nextID() string {
	f.seq++
	return fmt.Sprintf("id:%d", f.seq)
}

// ensureParents は親ディレクトリを作ります。
// アップロードは親を自動で作るという実物の振る舞いに合わせます。
func (f *fakeDropbox) ensureParents(p string) {
	parent := path.Dir(p)
	if parent == "/" || parent == "." || parent == "" {
		return
	}
	if f.entries[key(parent)] != nil {
		return
	}
	f.ensureParents(parent)
	f.entries[key(parent)] = &fakeEntry{path: parent, isDir: true, id: f.nextID()}
}

// children は直下の子を名前順に返します。
func (f *fakeDropbox) children(dir string) []*fakeEntry {
	out := []*fakeEntry{}
	for _, e := range f.entries {
		if path.Dir(e.path) == displayDir(dir) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

func displayDir(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// --- 各経路 ---

func (f *fakeDropbox) getMetadata(body json.RawMessage) (any, error) {
	var arg struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(body, &arg); err != nil {
		return nil, err
	}
	if arg.Path == "" {
		// 実物もルートのメタデータは扱えない。
		return nil, errNotFound()
	}

	e := f.get(arg.Path)
	if e == nil {
		return nil, errNotFound()
	}
	return f.metadataJSON(e), nil
}

func (f *fakeDropbox) listFolder(body json.RawMessage) (any, error) {
	var arg struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(body, &arg); err != nil {
		return nil, err
	}

	e := f.get(arg.Path)
	switch {
	case e == nil:
		return nil, errNotFound()
	case !e.isDir:
		return nil, errNotFolder()
	}

	cursor := &fakeCursor{entries: f.children(arg.Path)}
	return f.page(cursor), nil
}

func (f *fakeDropbox) listFolderContinue(body json.RawMessage) (any, error) {
	var arg struct {
		Cursor string `json:"cursor"`
	}
	if err := json.Unmarshal(body, &arg); err != nil {
		return nil, err
	}

	cursor, ok := f.cursors[arg.Cursor]
	if !ok {
		return nil, apiError{"reset/."}
	}
	delete(f.cursors, arg.Cursor)
	return f.page(cursor), nil
}

// page は続きの1ページぶんを返します。
func (f *fakeDropbox) page(c *fakeCursor) any {
	end := min(c.offset+f.pageSize, len(c.entries))

	entries := make([]any, 0, end-c.offset)
	for _, e := range c.entries[c.offset:end] {
		entries = append(entries, f.metadataJSON(e))
	}
	c.offset = end

	res := map[string]any{"entries": entries, "has_more": false, "cursor": ""}
	if c.offset < len(c.entries) {
		id := fmt.Sprintf("cursor:%d", len(f.cursors)+f.seq+1)
		f.seq++
		f.cursors[id] = c
		res["has_more"] = true
		res["cursor"] = id
	}
	return res
}

func (f *fakeDropbox) createFolder(body json.RawMessage) (any, error) {
	var arg struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(body, &arg); err != nil {
		return nil, err
	}

	if e := f.get(arg.Path); e != nil {
		if e.isDir {
			return nil, conflictErr("folder")
		}
		return nil, conflictErr("file")
	}

	// 実物と違い、親を自動では作りません。
	// 親をさかのぼって作り直す経路を必ず通すためです。
	// 実物が親を作ってくれる場合は最初の1回で済むので、
	// どちらの振る舞いでも動くことになります。
	parent := path.Dir(arg.Path)
	if parent != "/" && f.get(parent) == nil {
		return nil, errNotFound()
	}

	e := &fakeEntry{path: arg.Path, isDir: true, id: f.nextID()}
	f.entries[key(arg.Path)] = e
	return map[string]any{"metadata": f.metadataJSON(e)}, nil
}

func (f *fakeDropbox) delete(body json.RawMessage) (any, error) {
	var arg struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(body, &arg); err != nil {
		return nil, err
	}

	e := f.get(arg.Path)
	if e == nil || arg.Path == "" {
		return nil, errNotFound()
	}

	// 実物と同じく中身ごと消す。
	prefix := key(arg.Path) + "/"
	for k := range f.entries {
		if strings.HasPrefix(k, prefix) {
			delete(f.entries, k)
		}
	}
	delete(f.entries, key(arg.Path))

	return map[string]any{"metadata": f.metadataJSON(e)}, nil
}

func (f *fakeDropbox) relocate(remove bool) func(json.RawMessage) (any, error) {
	return func(body json.RawMessage) (any, error) {
		var arg struct {
			FromPath string `json:"from_path"`
			ToPath   string `json:"to_path"`
		}
		if err := json.Unmarshal(body, &arg); err != nil {
			return nil, err
		}

		from := f.get(arg.FromPath)
		if from == nil {
			return nil, errNotFound()
		}
		if f.get(arg.ToPath) != nil {
			return nil, toConflictErr()
		}
		f.ensureParents(arg.ToPath)

		moved := f.relocateOne(from, arg.FromPath, arg.ToPath, remove)
		for k, e := range f.entries {
			if strings.HasPrefix(k, key(arg.FromPath)+"/") {
				to := arg.ToPath + e.path[len(arg.FromPath):]
				f.relocateOne(e, e.path, to, remove)
			}
		}
		return map[string]any{"metadata": f.metadataJSON(moved)}, nil
	}
}

func (f *fakeDropbox) relocateOne(e *fakeEntry, from, to string, remove bool) *fakeEntry {
	copied := *e
	copied.path = to
	copied.id = f.nextID()
	f.entries[key(to)] = &copied
	if remove {
		delete(f.entries, key(from))
	}
	return &copied
}

func (f *fakeDropbox) download(w http.ResponseWriter, r *http.Request) {
	var arg struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	e := f.get(arg.Path)
	var md any
	var data []byte
	if e != nil && !e.isDir {
		md = f.metadataJSON(e)
		data = append([]byte(nil), e.data...)
	}
	f.mu.Unlock()

	switch {
	case e == nil:
		writeAPIError(w, errNotFound())
		return
	case e.isDir:
		writeAPIError(w, errNotFile())
		return
	}

	status := http.StatusOK
	if rng := r.Header.Get("Range"); rng != "" {
		var err error
		data, err = applyRange(data, rng)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		status = http.StatusPartialContent
	}

	result, _ := json.Marshal(md)
	w.Header().Set("Dropbox-API-Result", string(result))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// applyRange は "bytes=n-m" 形式の指定を適用します。
func applyRange(data []byte, spec string) ([]byte, error) {
	spec = strings.TrimPrefix(spec, "bytes=")
	lo, hi, found := strings.Cut(spec, "-")

	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("読み出し位置を解釈できません: %q", spec)
	}
	if start > int64(len(data)) {
		return nil, fmt.Errorf("読み出し位置が末尾を超えています: %d", start)
	}
	data = data[start:]

	if found && hi != "" {
		end, err := strconv.ParseInt(hi, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("読み出し範囲を解釈できません: %q", spec)
		}
		if n := end - start + 1; n < int64(len(data)) {
			data = data[:n]
		}
	}
	return data, nil
}

func (f *fakeDropbox) upload(w http.ResponseWriter, r *http.Request) {
	var arg dbx.UploadArg
	if err := json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 実物と同じく content_hash を照合する。
	// 通信の途中で内容が欠けた場合はここで弾かれる。
	if arg.ContentHash != "" {
		if got := contentHash(data); got != arg.ContentHash {
			writeAPIError(w, apiError{"payload_too_large/."})
			return
		}
	}

	f.mu.Lock()
	e := f.commit(arg.Path, data, arg.ClientModified)
	md := f.metadataJSON(e)
	f.mu.Unlock()

	writeJSON(w, md)
}

func (f *fakeDropbox) sessionStart(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	id := fmt.Sprintf("session:%d", len(f.sessions)+1)
	f.sessions[id] = &fakeSession{data: data}
	f.mu.Unlock()

	writeJSON(w, map[string]any{"session_id": id})
}

func (f *fakeDropbox) sessionAppend(w http.ResponseWriter, r *http.Request) {
	var arg struct {
		Cursor struct {
			SessionID string `json:"session_id"`
			Offset    uint64 `json:"offset"`
		} `json:"cursor"`
	}
	if err := json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sessions[arg.Cursor.SessionID]
	if !ok {
		writeAPIError(w, apiError{"not_found/."})
		return
	}
	if uint64(len(s.data)) != arg.Cursor.Offset {
		// 実物は正しい位置を添えて返すが、ここでは食い違い自体を失敗とする。
		writeAPIError(w, apiError{"incorrect_offset/."})
		return
	}
	s.data = append(s.data, data...)

	writeJSON(w, map[string]any{})
}

func (f *fakeDropbox) sessionFinish(w http.ResponseWriter, r *http.Request) {
	var arg struct {
		Cursor struct {
			SessionID string `json:"session_id"`
			Offset    uint64 `json:"offset"`
		} `json:"cursor"`
		Commit dbx.CommitInfo `json:"commit"`
	}
	if err := json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sessions[arg.Cursor.SessionID]
	if !ok {
		writeAPIError(w, apiError{"not_found/."})
		return
	}
	if uint64(len(s.data)) != arg.Cursor.Offset {
		writeAPIError(w, apiError{"incorrect_offset/."})
		return
	}

	content := append(s.data, data...)
	delete(f.sessions, arg.Cursor.SessionID)

	e := f.commit(arg.Commit.Path, content, arg.Commit.ClientModified)
	writeJSON(w, f.metadataJSON(e))
}

// commit はアップロードの結果を書き込みます。
func (f *fakeDropbox) commit(p string, data []byte, modified *dbxapi.DBXTime) *fakeEntry {
	f.ensureParents(p)

	mod := time.Now().UTC().Truncate(time.Second)
	if modified != nil {
		mod = time.Time(*modified)
	}

	e := &fakeEntry{path: p, data: data, modified: mod, id: f.nextID()}
	f.entries[key(p)] = e
	return e
}

// metadataJSON は1件をメタデータの JSON にします。
func (f *fakeDropbox) metadataJSON(e *fakeEntry) map[string]any {
	if e.isDir {
		return map[string]any{
			".tag":         "folder",
			"name":         path.Base(displayDir(e.path)),
			"id":           e.id,
			"path_lower":   key(e.path),
			"path_display": e.path,
		}
	}

	h := storage.NewDropboxContentHash()
	h.Write(e.data)

	return map[string]any{
		".tag":            "file",
		"name":            path.Base(e.path),
		"id":              e.id,
		"path_lower":      key(e.path),
		"path_display":    e.path,
		"size":            len(e.data),
		"rev":             "rev1",
		"client_modified": dbxapi.DBXTime(e.modified),
		"server_modified": dbxapi.DBXTime(e.modified),
		"content_hash":    hex.EncodeToString(h.Sum(nil)),
		"is_downloadable": true,
	}
}
