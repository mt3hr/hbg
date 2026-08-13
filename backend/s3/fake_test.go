package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// 偽の S3 サーバーです。
//
// docker も実際の入れ物もなしに、ディレクトリの見せかけ・分割送信・
// 利用者定義の項目・エラーの分類までを試験できるようにするためのものです。
//
// 実物と違って署名は確かめません。確かめたいのは hbg 側の振る舞いで、
// 署名の正しさは SDK の受け持ちだからです。

const testBucket = "試験用の入れ物"

// fakeObject は偽サーバー上の1件です。
type fakeObject struct {
	data        []byte
	meta        map[string]string
	lastMod     time.Time
	contentType string
	// etagOverride は分割して送られた場合の ETag です。
	// 実物と同じく、分割送信の ETag は中身の MD5 になりません。
	etagOverride string
}

func (o *fakeObject) etag() string {
	if o.etagOverride != "" {
		return o.etagOverride
	}
	sum := md5.Sum(o.data)
	return hex.EncodeToString(sum[:])
}

// fakeUpload は分割送信の途中経過です。
type fakeUpload struct {
	key   string
	meta  map[string]string
	parts map[int][]byte
}

// fakeS3 は S3 の API のごく一部を再現します。
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]*fakeObject
	uploads map[string]*fakeUpload
	seq     int

	// pageSize は一覧が1回に返す件数です。
	// 小さくしてあるので、続きの取得を必ず通ります。
	pageSize int

	failures map[string]*fakeFailure
	calls    map[string]int
}

type fakeFailure struct {
	remaining int
	status    int
	code      string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects:  map[string]*fakeObject{},
		uploads:  map[string]*fakeUpload{},
		pageSize: 3,
		failures: map[string]*fakeFailure{},
		calls:    map[string]int{},
	}
}

// start は偽サーバーを立ち上げ、そこへ向いたストレージを返します。
func (f *fakeS3) start(t *testing.T, mutate ...func(*Config)) *Storage {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	client := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(
			"試験用の鍵", "試験用の秘密", ""),
		// 実物の非 AWS 提供元に合わせて、要求された時だけ検査値を付ける。
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})

	cfg := Config{
		Name:           "偽s3",
		Provider:       ProviderMinIO,
		Bucket:         testBucket,
		ForcePathStyle: true,
		clientOverride: client,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("ストレージを作れません: %v", err)
	}
	return s
}

func (f *fakeS3) failNext(op string, n, status int, code string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[op] = &fakeFailure{remaining: n, status: status, code: code}
}

func (f *fakeS3) callCount(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

// objectCount は入っている件数を返します。
func (f *fakeS3) objectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key := splitRequestPath(r.URL.Path)
	if bucket != testBucket {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "入れ物がありません: "+bucket)
		return
	}

	op := operationName(r, key)

	f.mu.Lock()
	f.calls[op]++
	if fail, ok := f.failures[op]; ok && fail.remaining > 0 {
		fail.remaining--
		status, code := fail.status, fail.code
		f.mu.Unlock()
		writeS3Error(w, status, code, "わざと失敗させています")
		return
	}
	f.mu.Unlock()

	switch op {
	case "list":
		f.listObjects(w, r)
	case "delete_objects":
		f.deleteObjects(w, r)
	case "create_multipart":
		f.createMultipart(w, r, key)
	case "upload_part":
		f.uploadPart(w, r, key)
	case "complete_multipart":
		f.completeMultipart(w, r, key)
	case "abort_multipart":
		f.abortMultipart(w, r)
	case "copy":
		f.copyObject(w, r, key)
	case "put":
		f.putObject(w, r, key)
	case "get":
		f.getObject(w, r, key)
	case "head":
		f.headObject(w, key)
	case "delete":
		f.deleteObject(w, key)
	default:
		writeS3Error(w, http.StatusBadRequest, "MethodNotAllowed", "扱えない要求です: "+op)
	}
}

// splitRequestPath はパス形式の要求から入れ物と名前を取り出します。
func splitRequestPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	bucket, key, _ = strings.Cut(p, "/")

	// SDK は名前を URL 用に符号化して送ってくる。
	if decoded, err := url.PathUnescape(bucket); err == nil {
		bucket = decoded
	}
	if decoded, err := url.PathUnescape(key); err == nil {
		key = decoded
	}
	return bucket, key
}

// operationName は要求の種類を見分けます。
func operationName(r *http.Request, key string) string {
	q := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		if key == "" {
			return "list"
		}
		return "get"
	case http.MethodHead:
		return "head"
	case http.MethodPost:
		switch {
		case q.Has("delete"):
			return "delete_objects"
		case q.Has("uploads"):
			return "create_multipart"
		case q.Has("uploadId"):
			return "complete_multipart"
		}
	case http.MethodPut:
		switch {
		case q.Has("uploadId"):
			return "upload_part"
		case r.Header.Get("x-amz-copy-source") != "":
			return "copy"
		}
		return "put"
	case http.MethodDelete:
		if q.Has("uploadId") {
			return "abort_multipart"
		}
		return "delete"
	}
	return "unknown"
}

// --- 応答の組み立て ---

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{Code: code, Message: message})
}

func writeXML(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(v)
}

// --- 一覧 ---

type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	Delimiter             string   `xml:"Delimiter,omitempty"`
	KeyCount              int      `xml:"KeyCount"`
	MaxKeys               int      `xml:"MaxKeys"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	Contents              []listContents
	CommonPrefixes        []listCommonPrefix
}

type listContents struct {
	XMLName      xml.Name `xml:"Contents"`
	Key          string   `xml:"Key"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int64    `xml:"Size"`
	StorageClass string   `xml:"StorageClass"`
}

type listCommonPrefix struct {
	XMLName xml.Name `xml:"CommonPrefixes"`
	Prefix  string   `xml:"Prefix"`
}

func (f *fakeS3) listObjects(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	token := q.Get("continuation-token")

	f.mu.Lock()
	defer f.mu.Unlock()

	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	// 区切り文字が指定されていれば、その先をひとまとめにする。
	type item struct {
		key      string
		isPrefix bool
	}
	items := []item{}
	seen := map[string]bool{}
	for _, k := range keys {
		rest := strings.TrimPrefix(k, prefix)
		if delimiter != "" {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				group := prefix + rest[:idx+len(delimiter)]
				if !seen[group] {
					seen[group] = true
					items = append(items, item{key: group, isPrefix: true})
				}
				continue
			}
		}
		items = append(items, item{key: k})
	}

	// 続きの札を位置として使う。
	start := 0
	if token != "" {
		if n, err := strconv.Atoi(token); err == nil {
			start = n
		}
	}
	pageSize := f.pageSize
	if raw := q.Get("max-keys"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n < pageSize {
			pageSize = n
		}
	}
	end := min(start+pageSize, len(items))

	res := listBucketResult{
		Name:      testBucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   pageSize,
	}
	for _, it := range items[start:end] {
		if it.isPrefix {
			res.CommonPrefixes = append(res.CommonPrefixes, listCommonPrefix{Prefix: it.key})
			continue
		}
		obj := f.objects[it.key]
		res.Contents = append(res.Contents, listContents{
			Key:          it.key,
			LastModified: obj.lastMod.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + obj.etag() + `"`,
			Size:         int64(len(obj.data)),
			StorageClass: "STANDARD",
		})
	}
	res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)

	if end < len(items) {
		res.IsTruncated = true
		res.NextContinuationToken = strconv.Itoa(end)
	}

	writeXML(w, res)
}

// --- 1件の読み書き ---

// userMeta は要求から利用者定義の項目を取り出します。
func userMeta(r *http.Request) map[string]string {
	meta := map[string]string{}
	for name, values := range r.Header {
		lower := strings.ToLower(name)
		if key, ok := strings.CutPrefix(lower, "x-amz-meta-"); ok && len(values) > 0 {
			meta[key] = values[0]
		}
	}
	return meta
}

func (f *fakeS3) putObject(w http.ResponseWriter, r *http.Request, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	obj := &fakeObject{
		data:        data,
		meta:        userMeta(r),
		lastMod:     time.Now().UTC(),
		contentType: r.Header.Get("Content-Type"),
	}
	f.objects[key] = obj

	w.Header().Set("ETag", `"`+obj.etag()+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) getObject(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	obj, ok := f.objects[key]
	var data []byte
	if ok {
		data = append([]byte(nil), obj.data...)
	}
	f.mu.Unlock()

	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "ありません: "+key)
		return
	}

	status := http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		var err error
		data, err = applyRange(data, spec)
		if err != nil {
			writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", err.Error())
			return
		}
		status = http.StatusPartialContent
	}

	writeObjectHeaders(w, obj, len(data))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (f *fakeS3) headObject(w http.ResponseWriter, key string) {
	f.mu.Lock()
	obj, ok := f.objects[key]
	f.mu.Unlock()

	if !ok {
		writeS3Error(w, http.StatusNotFound, "NotFound", "ありません: "+key)
		return
	}

	writeObjectHeaders(w, obj, len(obj.data))
	w.WriteHeader(http.StatusOK)
}

func writeObjectHeaders(w http.ResponseWriter, obj *fakeObject, length int) {
	w.Header().Set("Content-Length", strconv.Itoa(length))
	w.Header().Set("Last-Modified", obj.lastMod.UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", `"`+obj.etag()+`"`)
	if obj.contentType != "" {
		w.Header().Set("Content-Type", obj.contentType)
	}
	for k, v := range obj.meta {
		w.Header().Set("x-amz-meta-"+k, v)
	}
}

func (f *fakeS3) deleteObject(w http.ResponseWriter, key string) {
	f.mu.Lock()
	delete(f.objects, key)
	f.mu.Unlock()

	// 実物も、無いものを消そうとしても成功として返す。
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) copyObject(w http.ResponseWriter, r *http.Request, key string) {
	source := r.Header.Get("x-amz-copy-source")
	if decoded, err := url.PathUnescape(source); err == nil {
		source = decoded
	}
	_, srcKey := splitRequestPath("/" + strings.TrimPrefix(source, "/"))

	f.mu.Lock()
	defer f.mu.Unlock()

	src, ok := f.objects[srcKey]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "コピー元がありません: "+srcKey)
		return
	}

	copied := &fakeObject{
		data:        append([]byte(nil), src.data...),
		meta:        map[string]string{},
		lastMod:     time.Now().UTC(),
		contentType: src.contentType,
	}
	for k, v := range src.meta {
		copied.meta[k] = v
	}
	f.objects[key] = copied

	writeXML(w, struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
	}{
		LastModified: copied.lastMod.UTC().Format("2006-01-02T15:04:05.000Z"),
		ETag:         `"` + copied.etag() + `"`,
	})
}

// --- まとめて削除 ---

func (f *fakeS3) deleteObjects(w http.ResponseWriter, r *http.Request) {
	var req struct {
		XMLName xml.Name `xml:"Delete"`
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}

	f.mu.Lock()
	for _, o := range req.Objects {
		delete(f.objects, o.Key)
	}
	f.mu.Unlock()

	writeXML(w, struct {
		XMLName xml.Name `xml:"DeleteResult"`
	}{})
}

// --- 分割送信 ---

func (f *fakeS3) createMultipart(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	f.seq++
	id := fmt.Sprintf("upload-%d", f.seq)
	f.uploads[id] = &fakeUpload{key: key, meta: userMeta(r), parts: map[int][]byte{}}
	f.mu.Unlock()

	writeXML(w, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{Bucket: testBucket, Key: key, UploadID: id})
}

func (f *fakeS3) uploadPart(w http.ResponseWriter, r *http.Request, _ string) {
	q := r.URL.Query()
	id := q.Get("uploadId")
	number, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidPart", "分割番号を解釈できません")
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	up, ok := f.uploads[id]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchUpload", "その送信は始まっていません")
		return
	}
	up.parts[number] = data

	sum := md5.Sum(data)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) completeMultipart(w http.ResponseWriter, r *http.Request, key string) {
	id := r.URL.Query().Get("uploadId")

	var req struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int `xml:"PartNumber"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	up, ok := f.uploads[id]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchUpload", "その送信は始まっていません")
		return
	}

	numbers := make([]int, 0, len(up.parts))
	for n := range up.parts {
		numbers = append(numbers, n)
	}
	sort.Ints(numbers)

	data := []byte{}
	for _, n := range numbers {
		data = append(data, up.parts[n]...)
	}
	delete(f.uploads, id)

	// 分割して送った場合、ETag は中身の MD5 にならない。
	// 各分割の MD5 を連ねたものの MD5 に、分割数を添えた形になる。
	digests := []byte{}
	for _, n := range numbers {
		sum := md5.Sum(up.parts[n])
		digests = append(digests, sum[:]...)
	}
	overall := md5.Sum(digests)
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(overall[:]), len(numbers))

	f.objects[key] = &fakeObject{
		data:         data,
		meta:         up.meta,
		lastMod:      time.Now().UTC(),
		etagOverride: etag,
	}

	writeXML(w, struct {
		XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket  string   `xml:"Bucket"`
		Key     string   `xml:"Key"`
		ETag    string   `xml:"ETag"`
	}{Bucket: testBucket, Key: key, ETag: `"` + etag + `"`})
}

func (f *fakeS3) abortMultipart(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	delete(f.uploads, r.URL.Query().Get("uploadId"))
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
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
