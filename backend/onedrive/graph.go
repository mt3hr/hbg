package onedrive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Microsoft Graph は OneDrive の窓口です。hbg が使うのは
// ファイルの一覧・取得・作成・削除・移動・コピーだけなので、
// 必要なところだけ自前で組み立てます。
//
// 公式の SDK を使わないのは、生成された部品が非常に大きく、
// 取り込むだけでビルドと試験の時間が桁違いに伸びるためです。
// hbg が触るのは10ほどの窓口なので、割に合いません。
//
// 認証は他のクラウドと同じ仕組み（internal/auth）に載せます。

// graphBase は Graph の入口です。
const graphBase = "https://graph.microsoft.com/v1.0"

// itemFields は1件について取得する項目です。
const itemFields = "id,name,size,folder,file,lastModifiedDateTime,fileSystemInfo,parentReference"

// listPageSize は一覧が1回に要求する件数です。
const listPageSize = 200

// graphClient は Graph とのやりとりです。
type graphClient struct {
	http *http.Client
	base string
	// driveRoot はドライブの入口です（"/me/drive" など）。
	driveRoot string
}

// driveItem は Graph が返すファイルやフォルダです。
type driveItem struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Folder *struct {
		ChildCount int64 `json:"childCount"`
	} `json:"folder"`
	File *struct {
		MimeType string `json:"mimeType"`
		Hashes   struct {
			QuickXorHash string `json:"quickXorHash"`
			SHA1Hash     string `json:"sha1Hash"`
			SHA256Hash   string `json:"sha256Hash"`
		} `json:"hashes"`
	} `json:"file"`
	LastModifiedDateTime string `json:"lastModifiedDateTime"`
	FileSystemInfo       *struct {
		CreatedDateTime      string `json:"createdDateTime,omitempty"`
		LastModifiedDateTime string `json:"lastModifiedDateTime,omitempty"`
	} `json:"fileSystemInfo"`
}

func (i driveItem) isDir() bool { return i.Folder != nil }

// modTime は元のファイルの更新時刻を返します。
//
// fileSystemInfo は書き込んだ側が伝えた時刻で、
// lastModifiedDateTime はサーバー上で変わった時刻です。
// 同期の判断に要るのは前者です。
func (i driveItem) modTime() time.Time {
	if i.FileSystemInfo != nil && i.FileSystemInfo.LastModifiedDateTime != "" {
		if t, err := time.Parse(time.RFC3339, i.FileSystemInfo.LastModifiedDateTime); err == nil {
			return t
		}
	}
	if t, err := time.Parse(time.RFC3339, i.LastModifiedDateTime); err == nil {
		return t
	}
	return time.Time{}
}

// itemsPage は一覧の1ページです。
type itemsPage struct {
	Value    []driveItem `json:"value"`
	NextLink string      `json:"@odata.nextLink"`
}

// --- 接続先の組み立て ---

// itemURL はパスに対応する項目の接続先を返します。
//
// Graph はパスによる指定を "root:/写真/a.jpg:" という形で表します。
// ルートだけは ":" を付けずに "root" と書きます。
func (c *graphClient) itemURL(p, suffix string) string {
	p = strings.Trim(p, "/")
	if p == "" {
		if suffix == "" {
			return c.base + c.driveRoot + "/root"
		}
		return c.base + c.driveRoot + "/root/" + suffix
	}

	escaped := escapePath(p)
	if suffix == "" {
		return c.base + c.driveRoot + "/root:/" + escaped
	}
	return c.base + c.driveRoot + "/root:/" + escaped + ":/" + suffix
}

// escapePath はパスを接続先に埋め込める形にします。
func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

// --- 要求の送信 ---

// request は1つの要求を送ります。応答は呼び出し側が閉じてください。
func (c *graphClient) request(
	ctx context.Context,
	method, rawURL string,
	body io.Reader,
	contentLength int64,
	headers map[string]string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = contentLength

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return c.http.Do(req)
}

// doJSON は要求を送り、応答を out に読み込みます。
func (c *graphClient) doJSON(ctx context.Context, method, rawURL string, in, out any) error {
	var body io.Reader
	length := int64(0)
	headers := map[string]string{"Accept": "application/json"}

	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
		length = int64(len(encoded))
		headers["Content-Type"] = "application/json"
	}

	res, err := c.request(ctx, method, rawURL, body, length, headers)
	if err != nil {
		return err
	}
	defer drain(res)

	if err := statusError(method, rawURL, res); err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

// drain は応答を読み捨てて閉じます。接続を使い回せるようにするためです。
func drain(res *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64*1024))
	_ = res.Body.Close()
}

// statusError は成功でない状態コードをエラーにします。
func statusError(method, rawURL string, res *http.Response) error {
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		return nil
	}

	e := &graphError{Method: method, URL: rawURL, Status: res.StatusCode}
	e.RetryAfter = parseRetryAfter(res.Header.Get("Retry-After"))

	// 応答の中身に理由が入っている。
	var payload struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 64*1024)).Decode(&payload); err == nil {
		e.Code = payload.Error.Code
		e.Message = payload.Error.Message
	}
	return e
}

// parseRetryAfter は待つよう指示された時間を読み取ります。
func parseRetryAfter(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	if seconds, err := time.ParseDuration(raw + "s"); err == nil {
		return seconds
	}
	if t, err := http.ParseTime(raw); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// graphError は Graph が返した失敗です。
type graphError struct {
	Method     string
	URL        string
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *graphError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s: %d %s (%s)", e.Method, e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %d %s", e.Method, e.Status, http.StatusText(e.Status))
}

// --- 各操作 ---

// getItem は1件のメタデータを取得します。
func (c *graphClient) getItem(ctx context.Context, p string) (*driveItem, error) {
	var item driveItem
	u := c.itemURL(p, "") + "?$select=" + url.QueryEscape(itemFields)
	if err := c.doJSON(ctx, http.MethodGet, u, nil, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// listChildren はディレクトリの直下を返します。
func (c *graphClient) listChildren(ctx context.Context, p string, fn func(driveItem) error) error {
	next := fmt.Sprintf("%s?$select=%s&$top=%d",
		c.itemURL(p, "children"), url.QueryEscape(itemFields), listPageSize)

	for next != "" {
		var page itemsPage
		if err := c.doJSON(ctx, http.MethodGet, next, nil, &page); err != nil {
			return err
		}
		for _, item := range page.Value {
			if err := fn(item); err != nil {
				return err
			}
		}
		next = page.NextLink
	}
	return nil
}

// getContent は内容を読み出します。
func (c *graphClient) getContent(ctx context.Context, p string, headers map[string]string) (io.ReadCloser, error) {
	res, err := c.request(ctx, http.MethodGet, c.itemURL(p, "content"), nil, 0, headers)
	if err != nil {
		return nil, err
	}
	if err := statusError(http.MethodGet, "content", res); err != nil {
		drain(res)
		return nil, err
	}
	return res.Body, nil
}

// createFolder はフォルダを1つ作ります。
func (c *graphClient) createFolder(ctx context.Context, parent, name string) (*driveItem, error) {
	body := map[string]any{
		"name":                              name,
		"folder":                            map[string]any{},
		"@microsoft.graph.conflictBehavior": "fail",
	}

	var item driveItem
	if err := c.doJSON(ctx, http.MethodPost, c.itemURL(parent, "children"), body, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// deleteItem は1件を削除します。フォルダの場合は中身ごと消えます。
func (c *graphClient) deleteItem(ctx context.Context, p string) error {
	return c.doJSON(ctx, http.MethodDelete, c.itemURL(p, ""), nil, nil)
}

// patchItem は項目の情報を書き換えます。移動や改名にも使います。
func (c *graphClient) patchItem(ctx context.Context, p string, body map[string]any) (*driveItem, error) {
	var item driveItem
	if err := c.doJSON(ctx, http.MethodPatch, c.itemURL(p, ""), body, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// uploadSmall は1回の要求で書き込みます。
func (c *graphClient) uploadSmall(ctx context.Context, p string, content []byte, modTime time.Time) (*driveItem, error) {
	res, err := c.request(ctx, http.MethodPut, c.itemURL(p, "content"),
		bytes.NewReader(content), int64(len(content)),
		map[string]string{"Content-Type": "application/octet-stream"})
	if err != nil {
		return nil, err
	}
	defer drain(res)

	if err := statusError(http.MethodPut, p, res); err != nil {
		return nil, err
	}

	var item driveItem
	if err := json.NewDecoder(res.Body).Decode(&item); err != nil {
		return nil, err
	}

	if modTime.IsZero() {
		return &item, nil
	}
	// 1回の要求で送る経路には時刻を添えられないので、あとから直す。
	return c.setModTime(ctx, p, modTime)
}

// setModTime は元のファイルの更新時刻を書き込みます。
func (c *graphClient) setModTime(ctx context.Context, p string, t time.Time) (*driveItem, error) {
	return c.patchItem(ctx, p, map[string]any{
		"fileSystemInfo": map[string]any{
			"lastModifiedDateTime": t.UTC().Format(time.RFC3339),
		},
	})
}

// createUploadSession は分割送信を始めます。送り先を返します。
func (c *graphClient) createUploadSession(ctx context.Context, p string, modTime time.Time) (string, error) {
	item := map[string]any{
		"@microsoft.graph.conflictBehavior": "replace",
		"name":                              path.Base(p),
	}
	if !modTime.IsZero() {
		item["fileSystemInfo"] = map[string]any{
			"lastModifiedDateTime": modTime.UTC().Format(time.RFC3339),
		}
	}

	var res struct {
		UploadURL string `json:"uploadUrl"`
	}
	if err := c.doJSON(ctx, http.MethodPost, c.itemURL(p, "createUploadSession"),
		map[string]any{"item": item}, &res); err != nil {
		return "", err
	}
	if res.UploadURL == "" {
		return "", fmt.Errorf("分割送信の送り先が返ってきませんでした")
	}
	return res.UploadURL, nil
}

// uploadChunk は分割送信の1つぶんを送ります。
//
// 最後のひとかたまりを送り終えると、書き込まれた項目が返ります。
func (c *graphClient) uploadChunk(
	ctx context.Context,
	uploadURL string,
	chunk []byte,
	offset, total int64,
) (*driveItem, error) {
	contentRange := fmt.Sprintf("bytes %d-%d/%d", offset, offset+int64(len(chunk))-1, total)

	// 送り先は署名済みの一時的な接続先なので、認証の情報を付けない。
	// 付けると拒否されることがある。
	res, err := c.request(ctx, http.MethodPut, uploadURL,
		bytes.NewReader(chunk), int64(len(chunk)),
		map[string]string{
			"Content-Range": contentRange,
			noAuthHeader:    "1",
		})
	if err != nil {
		return nil, err
	}
	defer drain(res)

	if err := statusError(http.MethodPut, uploadURL, res); err != nil {
		return nil, err
	}

	// 途中は 202 で、まだ項目は返らない。
	if res.StatusCode == http.StatusAccepted {
		return nil, nil
	}

	var item driveItem
	if err := json.NewDecoder(res.Body).Decode(&item); err != nil {
		return nil, err
	}
	return &item, nil
}

// cancelUploadSession は始めた分割送信を取り消します。
func (c *graphClient) cancelUploadSession(ctx context.Context, uploadURL string) {
	res, err := c.request(ctx, http.MethodDelete, uploadURL, nil, 0,
		map[string]string{noAuthHeader: "1"})
	if err != nil {
		return
	}
	drain(res)
}
