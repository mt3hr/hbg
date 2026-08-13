package onedrive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// 偽の Microsoft Graph サーバーです。
//
// 認証情報なしで、パスによる指定・続きの取得・分割送信・
// エラーの分類までを試験できるようにするためのものです。
//
// 実物と違って認証は確かめません。確かめたいのは hbg 側の
// 振る舞いで、トークンの扱いは internal/auth の受け持ちです。

// fakeItem は偽サーバー上の1件です。
type fakeItem struct {
	id      string
	name    string
	isDir   bool
	data    []byte
	modTime time.Time
	// fsModTime は書き込んだ側が伝えた更新時刻です。
	fsModTime time.Time
}

// fakeUpload は分割送信の途中経過です。
type fakeUpload struct {
	path string
	data []byte
	// fsModTime は送信を始めるときに伝えられた更新時刻です。
	fsModTime time.Time
}

// fakeGraph は Graph のごく一部を再現します。
type fakeGraph struct {
	mu sync.Mutex
	// items は正規化したパスをキーにした一覧です。
	// OneDrive は大文字小文字を区別しないため、実物に合わせます。
	items   map[string]*fakeItem
	uploads map[string]*fakeUpload
	seq     int

	// pageSize は一覧が1回に返す件数です。
	// 小さくしてあるので、続きの取得を必ず通ります。
	pageSize int

	failures map[string]*fakeFailure
	calls    map[string]int

	baseURL string
}

type fakeFailure struct {
	remaining int
	status    int
	code      string
	// retryAfter は Retry-After に入れる秒数です。
	retryAfter string
}

func newFakeGraph() *fakeGraph {
	return &fakeGraph{
		items:    map[string]*fakeItem{},
		uploads:  map[string]*fakeUpload{},
		pageSize: 3,
		failures: map[string]*fakeFailure{},
		calls:    map[string]int{},
	}
}

// start は偽サーバーを立ち上げ、そこへ向いたストレージを返します。
func (f *fakeGraph) start(t *testing.T, mutate ...func(*Config)) *Storage {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	f.baseURL = srv.URL

	cfg := Config{
		Name:         "偽onedrive",
		httpOverride: srv.Client(),
		baseOverride: srv.URL,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("ストレージを作れません: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func (f *fakeGraph) failNext(op string, n, status int, code string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[op] = &fakeFailure{remaining: n, status: status, code: code}
}

func (f *fakeGraph) failNextWithRetryAfter(op string, n, status int, code, retryAfter string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[op] = &fakeFailure{remaining: n, status: status, code: code, retryAfter: retryAfter}
}

func (f *fakeGraph) callCount(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

func (f *fakeGraph) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 分割送信の送り先は別扱い。
	if strings.HasPrefix(r.URL.Path, "/upload/") {
		f.handleUpload(w, r)
		return
	}

	itemPath, suffix, err := parseItemPath(r.URL.Path)
	if err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest", err.Error())
		return
	}

	op := operationName(r.Method, suffix)
	if f.injectFailure(w, op) {
		return
	}

	switch {
	case r.Method == http.MethodGet && suffix == "children":
		f.listChildren(w, r, itemPath)
	case r.Method == http.MethodGet && suffix == "content":
		f.getContent(w, r, itemPath)
	case r.Method == http.MethodGet && suffix == "":
		f.getItem(w, itemPath)
	case r.Method == http.MethodPost && suffix == "children":
		f.createChild(w, r, itemPath)
	case r.Method == http.MethodPost && suffix == "createUploadSession":
		f.createUploadSession(w, r, itemPath)
	case r.Method == http.MethodPut && suffix == "content":
		f.putContent(w, r, itemPath)
	case r.Method == http.MethodPatch && suffix == "":
		f.patchItem(w, r, itemPath)
	case r.Method == http.MethodDelete && suffix == "":
		f.deleteItem(w, itemPath)
	default:
		writeGraphError(w, http.StatusBadRequest, "invalidRequest",
			"扱えない要求です: "+r.Method+" "+r.URL.Path)
	}
}

// injectFailure は注入された失敗を返します。返したら真です。
func (f *fakeGraph) injectFailure(w http.ResponseWriter, op string) bool {
	f.mu.Lock()
	f.calls[op]++
	fail, ok := f.failures[op]
	if !ok || fail.remaining <= 0 {
		f.mu.Unlock()
		return false
	}
	fail.remaining--
	status, code, retryAfter := fail.status, fail.code, fail.retryAfter
	f.mu.Unlock()

	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	writeGraphError(w, status, code, "わざと失敗させています")
	return true
}

func operationName(method, suffix string) string {
	switch {
	case method == http.MethodGet && suffix == "children":
		return "list"
	case method == http.MethodGet && suffix == "content":
		return "download"
	case method == http.MethodGet:
		return "get"
	case method == http.MethodPost && suffix == "createUploadSession":
		return "create_session"
	case method == http.MethodPost:
		return "create"
	case method == http.MethodPut:
		return "upload"
	case method == http.MethodPatch:
		return "patch"
	case method == http.MethodDelete:
		return "delete"
	}
	return "unknown"
}

// parseItemPath は "/me/drive/root:/写真/a.jpg:/content" を読み解きます。
func parseItemPath(p string) (itemPath, suffix string, err error) {
	const marker = "/root"

	idx := strings.Index(p, marker)
	if idx < 0 {
		return "", "", fmt.Errorf("root を含まない接続先です: %s", p)
	}
	rest := p[idx+len(marker):]

	switch {
	case rest == "" || rest == "/":
		return "", "", nil
	case strings.HasPrefix(rest, "/"):
		// ルート直下の操作。"/children" など。
		return "", strings.TrimPrefix(rest, "/"), nil
	case !strings.HasPrefix(rest, ":"):
		return "", "", fmt.Errorf("解釈できない接続先です: %s", p)
	}

	// ":/写真/a.jpg" か ":/写真/a.jpg:/content" の形。
	rest = strings.TrimPrefix(rest, ":/")
	if before, after, found := strings.Cut(rest, ":/"); found {
		itemPath, suffix = before, after
	} else {
		itemPath = rest
	}

	decoded, err := url.PathUnescape(itemPath)
	if err != nil {
		return "", "", err
	}
	return strings.Trim(decoded, "/"), suffix, nil
}

// --- 応答の組み立て ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeGraphError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{"code": code, "message": message},
	})
}

// --- 蓄えの操作 ---

func key(p string) string { return strings.ToLower(strings.Trim(p, "/")) }

func (f *fakeGraph) get(p string) *fakeItem {
	if key(p) == "" {
		return &fakeItem{id: "root", name: "root", isDir: true}
	}
	return f.items[key(p)]
}

func (f *fakeGraph) nextID() string {
	f.seq++
	return fmt.Sprintf("item%d", f.seq)
}

// childCount は直下の件数を返します。
func (f *fakeGraph) childCount(p string) int64 {
	count := int64(0)
	for k := range f.items {
		if parentOf(k) == key(p) {
			count++
		}
	}
	return count
}

func parentOf(p string) string {
	dir := path.Dir(p)
	if dir == "." || dir == "/" {
		return ""
	}
	return dir
}

// itemJSON は1件をメタデータの JSON にします。
func (f *fakeGraph) itemJSON(p string, e *fakeItem) map[string]any {
	out := map[string]any{
		"id":                   e.id,
		"name":                 e.name,
		"lastModifiedDateTime": e.modTime.UTC().Format(time.RFC3339Nano),
	}

	if e.isDir {
		out["folder"] = map[string]any{"childCount": f.childCount(p)}
		return out
	}

	out["size"] = len(e.data)
	out["file"] = map[string]any{"mimeType": "application/octet-stream"}
	if !e.fsModTime.IsZero() {
		out["fileSystemInfo"] = map[string]any{
			"lastModifiedDateTime": e.fsModTime.UTC().Format(time.RFC3339Nano),
		}
	}
	return out
}

// --- 各操作 ---

func (f *fakeGraph) getItem(w http.ResponseWriter, itemPath string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := f.get(itemPath)
	if e == nil {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "ありません: "+itemPath)
		return
	}
	writeJSON(w, http.StatusOK, f.itemJSON(itemPath, e))
}

func (f *fakeGraph) listChildren(w http.ResponseWriter, r *http.Request, itemPath string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := f.get(itemPath)
	if e == nil {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "ありません: "+itemPath)
		return
	}
	if !e.isDir {
		writeGraphError(w, http.StatusBadRequest, "notAllowed", "ファイルの中身は一覧できません")
		return
	}

	paths := []string{}
	for k := range f.items {
		if parentOf(k) == key(itemPath) {
			paths = append(paths, k)
		}
	}
	sort.Strings(paths)

	skip := 0
	if raw := r.URL.Query().Get("$skip"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			skip = n
		}
	}
	end := min(skip+f.pageSize, len(paths))

	values := []any{}
	for _, p := range paths[skip:end] {
		values = append(values, f.itemJSON(p, f.items[p]))
	}

	res := map[string]any{"value": values}
	if end < len(paths) {
		res["@odata.nextLink"] = fmt.Sprintf("%s%s?$skip=%d", f.baseURL, r.URL.Path, end)
	}
	writeJSON(w, http.StatusOK, res)
}

func (f *fakeGraph) getContent(w http.ResponseWriter, r *http.Request, itemPath string) {
	f.mu.Lock()
	e := f.get(itemPath)
	var data []byte
	if e != nil && !e.isDir {
		data = append([]byte(nil), e.data...)
	}
	f.mu.Unlock()

	switch {
	case e == nil:
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "ありません: "+itemPath)
		return
	case e.isDir:
		writeGraphError(w, http.StatusBadRequest, "notAllowed", "フォルダは取り出せません")
		return
	}

	status := http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		var err error
		data, err = applyRange(data, spec)
		if err != nil {
			writeGraphError(w, http.StatusRequestedRangeNotSatisfiable, "invalidRange", err.Error())
			return
		}
		status = http.StatusPartialContent
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

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

func (f *fakeGraph) createChild(w http.ResponseWriter, r *http.Request, itemPath string) {
	var body struct {
		Name   string          `json:"name"`
		Folder json.RawMessage `json:"folder"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	parent := f.get(itemPath)
	if parent == nil {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "親がありません: "+itemPath)
		return
	}

	full := path.Join(itemPath, body.Name)
	if f.get(full) != nil {
		writeGraphError(w, http.StatusConflict, "nameAlreadyExists", "すでにあります: "+full)
		return
	}

	e := &fakeItem{
		id:      f.nextID(),
		name:    body.Name,
		isDir:   body.Folder != nil,
		modTime: time.Now().UTC(),
	}
	f.items[key(full)] = e

	writeJSON(w, http.StatusCreated, f.itemJSON(full, e))
}

func (f *fakeGraph) putContent(w http.ResponseWriter, r *http.Request, itemPath string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if parent := parentOf(key(itemPath)); parent != "" && f.get(parent) == nil {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "親がありません: "+parent)
		return
	}

	e := f.commit(itemPath, data, time.Time{})
	writeJSON(w, http.StatusCreated, f.itemJSON(itemPath, e))
}

func (f *fakeGraph) patchItem(w http.ResponseWriter, r *http.Request, itemPath string) {
	var body struct {
		Name           string `json:"name"`
		FileSystemInfo *struct {
			LastModifiedDateTime string `json:"lastModifiedDateTime"`
		} `json:"fileSystemInfo"`
		ParentReference *struct {
			Path string `json:"path"`
		} `json:"parentReference"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e := f.get(itemPath)
	if e == nil {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "ありません: "+itemPath)
		return
	}

	if body.FileSystemInfo != nil {
		if t, err := time.Parse(time.RFC3339, body.FileSystemInfo.LastModifiedDateTime); err == nil {
			e.fsModTime = t
		}
	}

	target := itemPath
	if body.ParentReference != nil {
		newParent := strings.TrimPrefix(body.ParentReference.Path, "/drive/root:")
		name := body.Name
		if name == "" {
			name = e.name
		}
		target = path.Join(strings.Trim(newParent, "/"), name)
	} else if body.Name != "" && body.Name != e.name {
		target = path.Join(parentOf(itemPath), body.Name)
	}

	if key(target) != key(itemPath) {
		if f.get(target) != nil {
			writeGraphError(w, http.StatusConflict, "nameAlreadyExists", "すでにあります: "+target)
			return
		}
		e.name = path.Base(target)
		delete(f.items, key(itemPath))
		f.items[key(target)] = e
	}

	writeJSON(w, http.StatusOK, f.itemJSON(target, e))
}

func (f *fakeGraph) deleteItem(w http.ResponseWriter, itemPath string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.get(itemPath) == nil {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "ありません: "+itemPath)
		return
	}

	// 実物と同じく中身ごと消す。
	prefix := key(itemPath) + "/"
	for k := range f.items {
		if strings.HasPrefix(k, prefix) {
			delete(f.items, k)
		}
	}
	delete(f.items, key(itemPath))

	w.WriteHeader(http.StatusNoContent)
}

// --- 分割送信 ---

func (f *fakeGraph) createUploadSession(w http.ResponseWriter, r *http.Request, itemPath string) {
	var body struct {
		Item struct {
			FileSystemInfo *struct {
				LastModifiedDateTime string `json:"lastModifiedDateTime"`
			} `json:"fileSystemInfo"`
		} `json:"item"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest", err.Error())
		return
	}

	fsModTime := time.Time{}
	if body.Item.FileSystemInfo != nil {
		if t, err := time.Parse(time.RFC3339, body.Item.FileSystemInfo.LastModifiedDateTime); err == nil {
			fsModTime = t
		}
	}

	f.mu.Lock()
	f.seq++
	id := fmt.Sprintf("session%d", f.seq)
	f.uploads[id] = &fakeUpload{path: itemPath, fsModTime: fsModTime}
	f.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"uploadUrl": f.baseURL + "/upload/" + id,
	})
}

func (f *fakeGraph) handleUpload(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/upload/")

	// 送り先は署名済みの接続先なので、認証の情報は付いていないはず。
	if r.Header.Get("Authorization") != "" {
		writeGraphError(w, http.StatusUnauthorized, "unauthenticated",
			"分割送信の送り先に認証の情報が付いています")
		return
	}

	if r.Method == http.MethodDelete {
		f.mu.Lock()
		delete(f.uploads, id)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if f.injectFailure(w, "upload_chunk") {
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest", err.Error())
		return
	}

	start, total, err := parseContentRange(r.Header.Get("Content-Range"))
	if err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalidRange", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	up, ok := f.uploads[id]
	if !ok {
		writeGraphError(w, http.StatusNotFound, "itemNotFound", "その送信は始まっていません")
		return
	}
	if start != int64(len(up.data)) {
		writeGraphError(w, http.StatusBadRequest, "invalidRange",
			fmt.Sprintf("送信位置が食い違っています（%d と %d）", start, len(up.data)))
		return
	}

	// 実物は 320KiB の倍数であることを求める（最後を除く）。
	if total < 0 && len(data)%chunkUnit != 0 {
		writeGraphError(w, http.StatusBadRequest, "invalidRequest",
			fmt.Sprintf("分割の大きさが %d の倍数ではありません: %d", chunkUnit, len(data)))
		return
	}

	up.data = append(up.data, data...)

	if total < 0 {
		// まだ続きがある。
		writeJSON(w, http.StatusAccepted, map[string]any{
			"nextExpectedRanges": []string{fmt.Sprintf("%d-", len(up.data))},
		})
		return
	}

	if int64(len(up.data)) != total {
		writeGraphError(w, http.StatusBadRequest, "invalidRange",
			fmt.Sprintf("全体の大きさが合いません（%d と %d）", len(up.data), total))
		return
	}

	delete(f.uploads, id)
	e := f.commit(up.path, up.data, up.fsModTime)
	writeJSON(w, http.StatusCreated, f.itemJSON(up.path, e))
}

// parseContentRange は "bytes 0-9/10" や "bytes 0-9/*" を読み取ります。
func parseContentRange(spec string) (start, total int64, err error) {
	spec = strings.TrimPrefix(spec, "bytes ")
	rng, totalRaw, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, 0, fmt.Errorf("Content-Range を解釈できません: %q", spec)
	}

	total = -1
	if totalRaw != "*" {
		if total, err = strconv.ParseInt(totalRaw, 10, 64); err != nil {
			return 0, 0, err
		}
	}

	lo, _, _ := strings.Cut(rng, "-")
	start, err = strconv.ParseInt(lo, 10, 64)
	return start, total, err
}

// commit は書き込みの結果を蓄えます。
func (f *fakeGraph) commit(p string, data []byte, fsModTime time.Time) *fakeItem {
	e := f.items[key(p)]
	if e == nil {
		e = &fakeItem{id: f.nextID(), name: path.Base(p)}
		f.items[key(p)] = e
	}
	e.data = data
	e.modTime = time.Now().UTC()
	if !fsModTime.IsZero() {
		e.fsModTime = fsModTime
	}
	return e
}
