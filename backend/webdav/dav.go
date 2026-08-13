package webdav

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// WebDAV は HTTP に手続きを足したものです。hbg が使うのは
// 次の7つだけなので、必要なところだけ自前で組み立てます。
//
//	PROPFIND  一覧とメタデータの取得
//	GET       読み出し
//	PUT       書き込み
//	MKCOL     ディレクトリの作成
//	DELETE    削除
//	MOVE      移動・改名
//	COPY      サーバー側でのコピー
//
// 既存の道具立てを使わない理由は2つあります。
//
//   - 書き込みのときに内容をいったんすべてメモリに載せる作りだった。
//     大きなファイルを扱う道具としては使えません。
//
//   - ファイルを1つ書くたびに、親ディレクトリへ MKCOL を投げていた。
//     要求が倍になるうえ、同じディレクトリへ並行して書くと
//     衝突します。
//
// どちらも手を入れられる作りではなかったので、ここで組み立てます。

// davClient は WebDAV サーバーとのやりとりです。
type davClient struct {
	base     *url.URL
	user     string
	password string
	http     *http.Client
}

// newDavClient はやりとりの相手を用意します。
func newDavClient(cfg Config) (*davClient, error) {
	base, err := url.Parse(strings.TrimSuffix(cfg.URL, "/") + "/")
	if err != nil {
		return nil, fmt.Errorf("url を解釈できません: %w", err)
	}

	transport := cfg.transportOverride
	if transport == nil {
		transport = http.DefaultTransport
	}

	return &davClient{
		base:     base,
		user:     cfg.User,
		password: cfg.Password,
		http: &http.Client{
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("転送が多すぎます")
				}
				if !mayFollowRedirect(via[0].Method) {
					// 書き込みや問い合わせの転送先は追わない。
					// PUT が転送されると、送り直しのときに GET に
					// なってしまうことがある。中身が消えかねない。
					return http.ErrUseLastResponse
				}
				return nil
			},
		},
	}, nil
}

// mayFollowRedirect は、その手続きで転送を追ってよいかを返します。
func mayFollowRedirect(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead:
		return true
	}
	return false
}

// urlFor はパスに対応する接続先を組み立てます。
func (c *davClient) urlFor(p string) string {
	u := *c.base
	u.Path = path.Join(u.Path, p)
	if strings.HasSuffix(p, "/") && !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String()
}

// request は1つの要求を送ります。応答は呼び出し側が閉じてください。
func (c *davClient) request(
	ctx context.Context,
	method, p string,
	body io.Reader,
	contentLength int64,
	headers map[string]string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.urlFor(p), body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = contentLength

	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return c.http.Do(req)
}

// do は要求を送り、応答の状態を確かめてから中身を捨てます。
func (c *davClient) do(ctx context.Context, method, p string, headers map[string]string) error {
	res, err := c.request(ctx, method, p, nil, 0, headers)
	if err != nil {
		return err
	}
	defer drain(res)

	return statusError(method, p, res.StatusCode)
}

// drain は応答を読み捨てて閉じます。接続を使い回せるようにするためです。
func drain(res *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64*1024))
	_ = res.Body.Close()
}

// statusError は成功でない状態コードをエラーにします。
func statusError(method, p string, status int) error {
	if status >= 200 && status <= 299 {
		return nil
	}
	return &davError{Method: method, Path: p, Status: status}
}

// davError は WebDAV サーバーが返した失敗です。
type davError struct {
	Method string
	Path   string
	Status int
}

func (e *davError) Error() string {
	return fmt.Sprintf("%s %s: %d %s", e.Method, e.Path, e.Status, http.StatusText(e.Status))
}

// --- 各手続き ---

// propfindBody は要求する項目です。
const propfindBody = `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:resourcetype/>
    <d:getcontentlength/>
    <d:getlastmodified/>
  </d:prop>
</d:propfind>`

// davEntry は PROPFIND で得られた1件です。
type davEntry struct {
	// href はサーバーが返したパスです（符号化されたまま）。
	href    string
	name    string
	isDir   bool
	size    int64
	modTime time.Time
}

// propfind はメタデータを問い合わせます。
//
// depth が 0 ならその1件だけ、1 なら直下も返ります。
func (c *davClient) propfind(ctx context.Context, p string, depth int) ([]davEntry, error) {
	res, err := c.request(ctx, "PROPFIND", p,
		strings.NewReader(propfindBody), int64(len(propfindBody)),
		map[string]string{
			"Depth":        fmt.Sprint(depth),
			"Content-Type": `application/xml; charset="utf-8"`,
		})
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	// 複数の結果は 207 で返る。それ以外は失敗として扱う。
	if res.StatusCode != http.StatusMultiStatus {
		return nil, statusError("PROPFIND", p, res.StatusCode)
	}

	var ms multistatus
	if err := xml.NewDecoder(res.Body).Decode(&ms); err != nil {
		return nil, fmt.Errorf("PROPFIND %s の応答を解釈できません: %w", p, err)
	}

	entries := make([]davEntry, 0, len(ms.Responses))
	for _, r := range ms.Responses {
		e, ok := r.entry()
		if !ok {
			continue
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// get は内容を読み出します。応答の中身は呼び出し側が閉じてください。
func (c *davClient) get(ctx context.Context, p string, headers map[string]string) (io.ReadCloser, error) {
	res, err := c.request(ctx, http.MethodGet, p, nil, 0, headers)
	if err != nil {
		return nil, err
	}

	if err := statusError(http.MethodGet, p, res.StatusCode); err != nil {
		drain(res)
		return nil, err
	}
	return res.Body, nil
}

// put は内容を書き込みます。
//
// contentLength が負なら長さを伝えずに送ります。
// 長さが分かっている場合は伝えます。実際に送られた量と食い違えば
// その場で失敗するので、黙って切り詰められることはありません。
func (c *davClient) put(ctx context.Context, p string, r io.Reader, contentLength int64, headers map[string]string) error {
	res, err := c.request(ctx, http.MethodPut, p, r, contentLength, headers)
	if err != nil {
		return err
	}
	defer drain(res)

	return statusError(http.MethodPut, p, res.StatusCode)
}

// mkcol はディレクトリを1つ作ります。
func (c *davClient) mkcol(ctx context.Context, p string) error {
	return c.do(ctx, "MKCOL", ensureSlash(p), nil)
}

// remove は1件を削除します。ディレクトリの場合は中身ごと消えます。
func (c *davClient) remove(ctx context.Context, p string) error {
	return c.do(ctx, http.MethodDelete, p, nil)
}

// move は移動・改名します。
func (c *davClient) move(ctx context.Context, from, to string, overwrite bool) error {
	return c.do(ctx, "MOVE", from, c.relocationHeaders(to, overwrite))
}

// copy はサーバー側でコピーします。
func (c *davClient) copy(ctx context.Context, from, to string, overwrite bool) error {
	return c.do(ctx, "COPY", from, c.relocationHeaders(to, overwrite))
}

func (c *davClient) relocationHeaders(to string, overwrite bool) map[string]string {
	flag := "F"
	if overwrite {
		flag = "T"
	}
	return map[string]string{
		"Destination": c.urlFor(to),
		"Overwrite":   flag,
	}
}

// ensureSlash はディレクトリを表すために末尾へ "/" を足します。
func ensureSlash(p string) string {
	if strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// --- 応答の解釈 ---

type multistatus struct {
	XMLName   xml.Name      `xml:"DAV: multistatus"`
	Responses []davResponse `xml:"DAV: response"`
}

type davResponse struct {
	Href     string     `xml:"DAV: href"`
	Propstat []propstat `xml:"DAV: propstat"`
}

type propstat struct {
	Status string  `xml:"DAV: status"`
	Prop   davProp `xml:"DAV: prop"`
}

type davProp struct {
	ResourceType  resourceType `xml:"DAV: resourcetype"`
	ContentLength *int64       `xml:"DAV: getcontentlength"`
	LastModified  string       `xml:"DAV: getlastmodified"`
}

type resourceType struct {
	Collection *struct{} `xml:"DAV: collection"`
}

// entry は1つの応答を davEntry にします。
// 取れなかった場合は false を返します。
func (r davResponse) entry() (davEntry, bool) {
	if r.Href == "" {
		return davEntry{}, false
	}

	e := davEntry{href: r.Href, size: -1}

	for _, ps := range r.Propstat {
		// "HTTP/1.1 200 OK" の形。取れなかった項目は別の状態で返る。
		if !strings.Contains(ps.Status, " 200 ") {
			continue
		}

		if ps.Prop.ResourceType.Collection != nil {
			e.isDir = true
		}
		if ps.Prop.ContentLength != nil {
			e.size = *ps.Prop.ContentLength
		}
		if ps.Prop.LastModified != "" {
			// getlastmodified は HTTP の日付の形。
			if t, err := http.ParseTime(ps.Prop.LastModified); err == nil {
				e.modTime = t
			}
		}
	}

	e.name = nameFromHref(r.Href)
	if e.name == "" {
		return davEntry{}, false
	}
	return e, true
}

// nameFromHref は応答のパスから名前を取り出します。
func nameFromHref(href string) string {
	// href は接続先そのものの場合と、パスだけの場合がある。
	if u, err := url.Parse(href); err == nil {
		href = u.Path
	}

	href = strings.TrimSuffix(href, "/")
	if href == "" {
		return ""
	}
	return path.Base(href)
}
