package googledrive

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// 偽の Google Drive サーバーです。
//
// 認証情報なしで、ページ分割・パスの解決・分割送信・エラーの分類までを
// 試験できるようにするためのものです。Drive のクライアントは
// option.WithEndpoint で到達先を差し替えられます。
//
// Drive はパスではなくIDの網なので、この偽サーバーもIDで持ちます。
// 「同じ名前のものが2つ作れてしまう」という実物の性質も再現します。

// fakeFile は偽サーバー上の1件です。
type fakeFile struct {
	id       string
	name     string
	mimeType string
	parents  []string
	modified time.Time
	data     []byte
	trashed  bool
}

// fakeSession は分割送信の途中経過です。
type fakeSession struct {
	fileID string
	file   *drive.File
	data   []byte
}

// fakeDrive は Drive API のごく一部を再現します。
type fakeDrive struct {
	mu       sync.Mutex
	files    map[string]*fakeFile
	sessions map[string]*fakeSession
	tokens   map[string][]*fakeFile
	seq      int

	// pageSize は一覧が1回に返す件数です。
	// 小さくしてあるので、続きの取得を必ず通ります。
	pageSize int

	// failures は経路ごとの「あと何回失敗させるか」です。
	failures map[string]*fakeFailure
	calls    map[string]int

	baseURL string
}

type fakeFailure struct {
	remaining int
	status    int
	body      string
}

func newFakeDrive() *fakeDrive {
	return &fakeDrive{
		files:    map[string]*fakeFile{},
		sessions: map[string]*fakeSession{},
		tokens:   map[string][]*fakeFile{},
		pageSize: 3,
		failures: map[string]*fakeFailure{},
		calls:    map[string]int{},
	}
}

// start は偽サーバーを立ち上げ、そこへ向いたストレージを返します。
func (f *fakeDrive) start(t *testing.T) *Storage {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	f.baseURL = srv.URL

	svc, err := drive.NewService(context.Background(),
		option.WithEndpoint(srv.URL+"/"),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("偽サーバーへ向けたクライアントを作れません: %v", err)
	}

	s, err := newWithService(Config{Name: "偽drive"}, svc)
	if err != nil {
		t.Fatalf("ストレージを作れません: %v", err)
	}
	return s
}

func (f *fakeDrive) failNext(route string, n, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[route] = &fakeFailure{remaining: n, status: status, body: body}
}

func (f *fakeDrive) callCount(route string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[route]
}

var (
	filesIDRe   = regexp.MustCompile(`^/files/([^/]+)$`)
	filesCopyRe = regexp.MustCompile(`^/files/([^/]+)/copy$`)
	uploadIDRe  = regexp.MustCompile(`^/upload/drive/v3/files/([^/]+)$`)
	sessionRe   = regexp.MustCompile(`^/resumable/([^/]+)$`)
)

func (f *fakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := f.routeName(r)

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

	p := r.URL.Path
	switch {
	case p == "/files" && r.Method == http.MethodGet:
		f.list(w, r)
	case p == "/files" && r.Method == http.MethodPost:
		f.createMetadata(w, r)
	case p == "/upload/drive/v3/files":
		f.upload(w, r, "")
	case uploadIDRe.MatchString(p):
		f.upload(w, r, uploadIDRe.FindStringSubmatch(p)[1])
	case sessionRe.MatchString(p):
		f.uploadChunk(w, r, sessionRe.FindStringSubmatch(p)[1])
	case filesCopyRe.MatchString(p):
		f.copyFile(w, r, filesCopyRe.FindStringSubmatch(p)[1])
	case filesIDRe.MatchString(p):
		f.fileByID(w, r, filesIDRe.FindStringSubmatch(p)[1])
	default:
		writeError(w, http.StatusNotFound, "notFound", "知らない経路です: "+r.Method+" "+p)
	}
}

// routeName は失敗の注入と回数の集計に使う名前です。
func (f *fakeDrive) routeName(r *http.Request) string {
	p := r.URL.Path
	switch {
	case p == "/files" && r.Method == http.MethodGet:
		return "list"
	case p == "/files":
		return "create"
	case strings.HasPrefix(p, "/upload/"):
		return "upload"
	case strings.HasPrefix(p, "/resumable/"):
		return "upload_chunk"
	case filesCopyRe.MatchString(p):
		return "copy"
	case filesIDRe.MatchString(p):
		if r.URL.Query().Get("alt") == "media" {
			return "download"
		}
		return strings.ToLower(r.Method) + "_file"
	}
	return "unknown"
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, reason, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"errors":  []map[string]any{{"reason": reason, "message": message}},
		},
	})
}

// --- 蓄えの操作 ---

func (f *fakeDrive) nextID(prefix string) string {
	f.seq++
	return fmt.Sprintf("%s%d", prefix, f.seq)
}

func (f *fakeDrive) toDriveFile(e *fakeFile) *drive.File {
	out := &drive.File{
		Id:           e.id,
		Name:         e.name,
		MimeType:     e.mimeType,
		Parents:      e.parents,
		Trashed:      e.trashed,
		ModifiedTime: e.modified.UTC().Format(time.RFC3339Nano),
	}
	if e.mimeType != folderMIME && !isNative(e.mimeType) {
		out.Size = int64(len(e.data))
		md5sum := md5.Sum(e.data)
		sha1sum := sha1.Sum(e.data)
		sha256sum := sha256.Sum256(e.data)
		out.Md5Checksum = hex.EncodeToString(md5sum[:])
		out.Sha1Checksum = hex.EncodeToString(sha1sum[:])
		out.Sha256Checksum = hex.EncodeToString(sha256sum[:])
	}
	return out
}

// --- 一覧 ---

// queryRe は偽サーバーが解釈する検索式です。
// 実物のような一般の検索式ではなく、hbg が組み立てる形だけを受け付けます。
var (
	parentRe = regexp.MustCompile(`'((?:[^'\\]|\\.)*)' in parents`)
	nameRe   = regexp.MustCompile(`name = '((?:[^'\\]|\\.)*)'`)
)

func (f *fakeDrive) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	f.mu.Lock()
	defer f.mu.Unlock()

	var matched []*fakeFile
	if token := q.Get("pageToken"); token != "" {
		rest, ok := f.tokens[token]
		if !ok {
			writeError(w, http.StatusBadRequest, "invalidPageToken", "続きの札が無効です")
			return
		}
		delete(f.tokens, token)
		matched = rest
	} else {
		var err error
		matched, err = f.selectFiles(q.Get("q"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalidQuery", err.Error())
			return
		}
	}

	pageSize := f.pageSize
	if n, err := strconv.Atoi(q.Get("pageSize")); err == nil && n > 0 && n < pageSize {
		pageSize = n
	}

	res := &drive.FileList{}
	end := min(pageSize, len(matched))
	for _, e := range matched[:end] {
		res.Files = append(res.Files, f.toDriveFile(e))
	}
	if end < len(matched) {
		token := f.nextID("token")
		f.tokens[token] = matched[end:]
		res.NextPageToken = token
	}

	writeJSON(w, res)
}

// selectFiles は検索式に合うものを返します。
func (f *fakeDrive) selectFiles(query string) ([]*fakeFile, error) {
	m := parentRe.FindStringSubmatch(query)
	if m == nil {
		return nil, fmt.Errorf("親の指定がない検索式は扱えません: %q", query)
	}
	parent := unescapeQuery(m[1])

	name := ""
	if m := nameRe.FindStringSubmatch(query); m != nil {
		name = unescapeQuery(m[1])
	}
	wantUntrashed := strings.Contains(query, "trashed = false")

	out := []*fakeFile{}
	for _, e := range f.files {
		switch {
		case !slicesContains(e.parents, parent):
			continue
		case name != "" && e.name != name:
			continue
		case wantUntrashed && e.trashed:
			continue
		}
		out = append(out, e)
	}

	// 実物は API が定める順で返す。実行のたびに変わらないよう並べる。
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

func slicesContains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func unescapeQuery(s string) string {
	s = strings.ReplaceAll(s, `\'`, `'`)
	return strings.ReplaceAll(s, `\\`, `\`)
}

// --- 1件の操作 ---

func (f *fakeDrive) fileByID(w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("alt") == "media" {
			f.download(w, r, id)
			return
		}
		f.getMetadata(w, id)
	case http.MethodPatch:
		f.patch(w, r, id)
	case http.MethodDelete:
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := f.files[id]; !ok {
			writeError(w, http.StatusNotFound, "notFound", "そのIDはありません: "+id)
			return
		}
		delete(f.files, id)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "notAllowed", r.Method)
	}
}

func (f *fakeDrive) getMetadata(w http.ResponseWriter, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.files[id]
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "そのIDはありません: "+id)
		return
	}
	writeJSON(w, f.toDriveFile(e))
}

func (f *fakeDrive) download(w http.ResponseWriter, r *http.Request, id string) {
	f.mu.Lock()
	e, ok := f.files[id]
	var data []byte
	if ok {
		data = append([]byte(nil), e.data...)
	}
	f.mu.Unlock()

	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "そのIDはありません: "+id)
		return
	}

	status := http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		var err error
		data, err = applyRange(data, spec)
		if err != nil {
			writeError(w, http.StatusRequestedRangeNotSatisfiable, "badRange", err.Error())
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

func (f *fakeDrive) createMetadata(w http.ResponseWriter, r *http.Request) {
	var meta drive.File
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e := f.create(&meta, nil)
	writeJSON(w, f.toDriveFile(e))
}

func (f *fakeDrive) patch(w http.ResponseWriter, r *http.Request, id string) {
	var meta drive.File
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.files[id]
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "そのIDはありません: "+id)
		return
	}

	f.applyMeta(e, &meta, r.URL.Query())
	writeJSON(w, f.toDriveFile(e))
}

func (f *fakeDrive) applyMeta(e *fakeFile, meta *drive.File, q map[string][]string) {
	if meta.Name != "" {
		e.name = meta.Name
	}
	if meta.Trashed {
		e.trashed = true
	}
	if meta.ModifiedTime != "" {
		if t, err := time.Parse(time.RFC3339, meta.ModifiedTime); err == nil {
			e.modified = t
		}
	}
	for _, add := range q["addParents"] {
		e.parents = append(e.parents, strings.Split(add, ",")...)
	}
	for _, remove := range q["removeParents"] {
		for _, id := range strings.Split(remove, ",") {
			e.parents = removeString(e.parents, id)
		}
	}
}

func removeString(list []string, v string) []string {
	out := list[:0]
	for _, s := range list {
		if s != v {
			out = append(out, s)
		}
	}
	return out
}

func (f *fakeDrive) create(meta *drive.File, data []byte) *fakeFile {
	mimeType := meta.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	modified := time.Now().UTC()
	if meta.ModifiedTime != "" {
		if t, err := time.Parse(time.RFC3339, meta.ModifiedTime); err == nil {
			modified = t
		}
	}

	e := &fakeFile{
		id:       f.nextID("file"),
		name:     meta.Name,
		mimeType: mimeType,
		parents:  meta.Parents,
		modified: modified,
		data:     data,
	}
	if len(e.parents) == 0 {
		e.parents = []string{"root"}
	}
	f.files[e.id] = e
	return e
}

func (f *fakeDrive) copyFile(w http.ResponseWriter, r *http.Request, id string) {
	var meta drive.File
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	src, ok := f.files[id]
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "そのIDはありません: "+id)
		return
	}

	meta.MimeType = src.mimeType
	if meta.Name == "" {
		meta.Name = src.name
	}
	e := f.create(&meta, append([]byte(nil), src.data...))
	e.modified = src.modified

	writeJSON(w, f.toDriveFile(e))
}

// --- 送信 ---

func (f *fakeDrive) upload(w http.ResponseWriter, r *http.Request, id string) {
	switch r.URL.Query().Get("uploadType") {
	case "multipart":
		f.uploadMultipart(w, r, id)
	case "resumable":
		f.uploadStart(w, r, id)
	default:
		writeError(w, http.StatusBadRequest, "badRequest",
			"扱えない uploadType です: "+r.URL.Query().Get("uploadType"))
	}
}

func (f *fakeDrive) uploadMultipart(w http.ResponseWriter, r *http.Request, id string) {
	meta, data, err := readMultipart(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e, err := f.commit(id, meta, data)
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", err.Error())
		return
	}
	writeJSON(w, f.toDriveFile(e))
}

// readMultipart はメタデータと中身に分かれた本体を読みます。
func readMultipart(r *http.Request) (*drive.File, []byte, error) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, nil, err
	}

	mr := multipart.NewReader(r.Body, params["boundary"])

	metaPart, err := mr.NextPart()
	if err != nil {
		return nil, nil, fmt.Errorf("メタデータの部分がありません: %w", err)
	}
	var meta drive.File
	if err := json.NewDecoder(metaPart).Decode(&meta); err != nil {
		return nil, nil, err
	}

	dataPart, err := mr.NextPart()
	if err != nil {
		return nil, nil, fmt.Errorf("中身の部分がありません: %w", err)
	}
	data, err := io.ReadAll(dataPart)
	if err != nil {
		return nil, nil, err
	}
	return &meta, data, nil
}

func (f *fakeDrive) uploadStart(w http.ResponseWriter, r *http.Request, id string) {
	var meta drive.File
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}

	f.mu.Lock()
	session := f.nextID("session")
	f.sessions[session] = &fakeSession{fileID: id, file: &meta}
	f.mu.Unlock()

	w.Header().Set("Location", f.baseURL+"/resumable/"+session)
	writeJSON(w, map[string]any{})
}

func (f *fakeDrive) uploadChunk(w http.ResponseWriter, r *http.Request, session string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sessions[session]
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "その送信は始まっていません")
		return
	}

	contentRange := r.Header.Get("Content-Range")
	start, final, err := parseContentRange(contentRange)
	if err != nil {
		writeError(w, http.StatusBadRequest, "badRequest", err.Error())
		return
	}
	if start != int64(len(s.data)) {
		writeError(w, http.StatusBadRequest, "badRequest",
			fmt.Sprintf("送信位置が食い違っています（%d と %d）", start, len(s.data)))
		return
	}
	s.data = append(s.data, data...)

	if !final {
		// 実物は 308 を返すが、クライアントが X-GUploader-No-308 を
		// 付けてくるので、200 と上書き用のヘッダで返す。
		w.Header().Set("X-Http-Status-Code-Override", "308")
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(s.data)-1))
		w.WriteHeader(http.StatusOK)
		return
	}

	delete(f.sessions, session)

	e, err := f.commit(s.fileID, s.file, s.data)
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", err.Error())
		return
	}
	writeJSON(w, f.toDriveFile(e))
}

// parseContentRange は "bytes 0-9/10" や "bytes 0-9/*" を読み取ります。
func parseContentRange(spec string) (start int64, final bool, err error) {
	spec = strings.TrimPrefix(spec, "bytes ")
	rng, total, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, false, fmt.Errorf("Content-Range を解釈できません: %q", spec)
	}
	final = total != "*"

	if rng == "*" {
		// 中身が空のまま終わる場合。
		n, err := strconv.ParseInt(total, 10, 64)
		return n, true, err
	}

	lo, _, _ := strings.Cut(rng, "-")
	start, err = strconv.ParseInt(lo, 10, 64)
	return start, final, err
}

// commit は送信された中身を書き込みます。
func (f *fakeDrive) commit(id string, meta *drive.File, data []byte) (*fakeFile, error) {
	if id == "" {
		return f.create(meta, data), nil
	}

	e, ok := f.files[id]
	if !ok {
		return nil, fmt.Errorf("そのIDはありません: %s", id)
	}
	e.data = data
	f.applyMeta(e, meta, nil)
	return e, nil
}
