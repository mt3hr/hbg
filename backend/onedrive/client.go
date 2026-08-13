package onedrive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mt3hr/hbg/internal/auth"
	"golang.org/x/oauth2"
)

// ドライブの種類。
const (
	// DriveTypePersonal は個人用の OneDrive です。
	DriveTypePersonal = "personal"
	// DriveTypeBusiness は職場・学校の OneDrive です。
	DriveTypeBusiness = "business"
	// DriveTypeSharePoint は SharePoint のドキュメントライブラリです。
	DriveTypeSharePoint = "sharepoint"
)

// noAuthHeader は「この要求に認証を付けない」という印です。
//
// 分割送信の送り先は署名済みの一時的な接続先で、認証の情報を
// 付けると拒否されることがあります。送る直前に取り除きます。
const noAuthHeader = "X-Hbg-No-Auth"

// Config は OneDrive ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。
	Name string

	// ClientID は Microsoft のアプリ（クライアント）IDです。
	// 省略時は環境変数やビルド時に埋め込まれた値が使われます。
	ClientID string
	// Tenant は組織の識別子です。省略すると個人用と職場用の両方を受け付けます。
	Tenant string

	// DriveType はドライブの種類です。
	// "personal"（既定）、"business"、"sharepoint" のいずれかです。
	DriveType string
	// DriveID を指定すると、そのドライブを使います。
	DriveID string
	// SiteID は SharePoint のサイトです。drive_type が sharepoint のときに使います。
	SiteID string

	// Root を指定すると、その下を起点として扱います。
	Root string

	// httpOverride は試験のために通信の相手を差し替えるためのものです。
	httpOverride *http.Client
	// baseOverride は試験のために入口を差し替えるためのものです。
	baseOverride string
}

func (c Config) driveType() string {
	if c.DriveType == "" {
		return DriveTypePersonal
	}
	return c.DriveType
}

// validate は接続を試みる前に設定の不足を知らせます。
func (c Config) validate() error {
	switch c.driveType() {
	case DriveTypePersonal, DriveTypeBusiness:
	case DriveTypeSharePoint:
		if c.SiteID == "" && c.DriveID == "" {
			return errors.New("drive_type が sharepoint のときは site_id か drive_id が必要です")
		}
	default:
		return fmt.Errorf("drive_type には %q, %q, %q のいずれかを指定してください（%q が指定されました）",
			DriveTypePersonal, DriveTypeBusiness, DriveTypeSharePoint, c.DriveType)
	}
	return nil
}

// driveRoot はドライブの入口を組み立てます。
func (c Config) driveRoot() string {
	switch {
	case c.DriveID != "":
		return "/drives/" + c.DriveID
	case c.driveType() == DriveTypeSharePoint:
		return "/sites/" + c.SiteID + "/drive"
	}
	return "/me/drive"
}

// oauth2Config は設定から oauth2.Config を組み立てます。
//
// PKCE を使うのでクライアントシークレットは不要です。
func oauth2Config(cfg Config) (*oauth2.Config, error) {
	creds, err := auth.ResolveMicrosoft(cfg.ClientID)
	if err != nil {
		return nil, err
	}
	return auth.MicrosoftOAuth2Config(creds, cfg.Tenant), nil
}

// Login は対話的に認可を行い、トークンを保存します。
// hbg auth login から呼ばれます。
func Login(ctx context.Context, cfg Config, opts auth.LoginOptions) error {
	oauthCfg, err := oauth2Config(cfg)
	if err != nil {
		return err
	}

	flow := &auth.Flow{
		Config: oauthCfg,
		// パブリッククライアントなので PKCE が必須です。
		UsePKCE: true,
		// Microsoft は 127.0.0.1 なら任意のポートを受け付けます。
		OpenBrowser: opts.OpenBrowser,
		Prompt:      opts.Prompt,
	}

	tok, err := flow.Run(ctx)
	if err != nil {
		return err
	}
	if tok.RefreshToken == "" {
		// offline_access を要求していれば必ず得られるはず。
		// 得られない場合、アクセストークンは1時間ほどで失効してしまう。
		return errors.New("リフレッシュトークンを取得できませんでした。" +
			"アプリの登録で offline_access が許可されているか確認してください")
	}

	return auth.NewFileStore().Save(Type, cfg.Name, tok)
}

// newHTTPClient は認証を付ける HTTP のやりとりを用意します。
func newHTTPClient(ctx context.Context, cfg Config) (*http.Client, error) {
	if cfg.httpOverride != nil {
		return cfg.httpOverride, nil
	}

	oauthCfg, err := oauth2Config(cfg)
	if err != nil {
		return nil, err
	}

	store := auth.NewFileStore()
	tok, err := store.Load(Type, cfg.Name)
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			return nil, fmt.Errorf("onedrive %q は未認証です。hbg auth login %s で認証してください", cfg.Name, cfg.Name)
		}
		return nil, err
	}

	if tok.RefreshToken == "" {
		return nil, fmt.Errorf("onedrive %q のトークンは更新できません。"+
			"hbg auth login %s で認証をやり直してください", cfg.Name, cfg.Name)
	}

	// アクセストークンは1時間ほどで失効するが、リフレッシュトークンから
	// 自動更新され、更新結果はディスクへ書き戻される。
	src := auth.PersistingTokenSource(
		oauthCfg.TokenSource(ctx, tok), store, Type, cfg.Name, tok)

	client := oauth2.NewClient(ctx, src)
	client.Transport = &noAuthTransport{inner: client.Transport}
	client.CheckRedirect = checkRedirect
	return client, nil
}

// noAuthTransport は印の付いた要求から認証の情報を取り除きます。
type noAuthTransport struct {
	inner http.RoundTripper
}

func (t *noAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get(noAuthHeader) == "" {
		return t.inner.RoundTrip(req)
	}

	// 差し替えた要求を使う。元の要求は書き換えない決まりのため。
	stripped := req.Clone(req.Context())
	stripped.Header.Del(noAuthHeader)
	stripped.Header.Del("Authorization")

	// 認証を付ける層を通さずに送る。
	return http.DefaultTransport.RoundTrip(stripped)
}

// checkRedirect は転送のときの決まりです。
//
// 内容の取得は署名付きの別の場所へ転送されます。そこへ認証の情報を
// そのまま持っていくと拒否されるので、行き先が変わったら外します。
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return fmt.Errorf("転送が多すぎます")
	}
	if !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
		req.Header.Del("Authorization")
	}
	return nil
}
