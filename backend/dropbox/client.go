package dropbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	dbxapi "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/retry"
	"github.com/mt3hr/hbg/internal/auth"
	"golang.org/x/oauth2"
)

// Config は Dropbox ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。コマンドで "名前:パス" として使います。
	Name string
	// AppKey は Dropbox アプリのキーです。
	// 空の場合は環境変数やビルド時に埋め込まれた値が使われます。
	AppKey string
}

// oauth2Config は設定から oauth2.Config を組み立てます。
//
// PKCE を使うためクライアントシークレットは不要です。
func oauth2Config(cfg Config) (*oauth2.Config, error) {
	creds, err := auth.ResolveDropbox(cfg.AppKey)
	if err != nil {
		return nil, err
	}
	return auth.DropboxOAuth2Config(creds), nil
}

// loginFlow は認可フローを組み立てます。
//
// Login から切り出してあるのは、リダイレクト URI の指定が
// アプリ登録時の値と一致していることをテストから確かめるためです。
func loginFlow(oauthCfg *oauth2.Config, opts auth.LoginOptions) *auth.Flow {
	return &auth.Flow{
		Config:  oauthCfg,
		UsePKCE: true,
		// Dropbox はリダイレクトURIの完全一致を要求するため、
		// アプリ登録時に設定したホストとポートを使う。
		// 既定の 127.0.0.1 のままだと invalid redirect uri になる。
		FixedPorts:           auth.DropboxRedirectPorts,
		RedirectHost:         auth.DropboxRedirectHost,
		ExtraAuthCodeOptions: auth.DropboxAuthCodeOptions(),
		OpenBrowser:          opts.OpenBrowser,
		Prompt:               opts.Prompt,
	}
}

// Login は対話的に認可を行い、トークンを保存します。
// hbg auth login から呼ばれます。
func Login(ctx context.Context, cfg Config, opts auth.LoginOptions) error {
	oauthCfg, err := oauth2Config(cfg)
	if err != nil {
		return err
	}

	tok, err := loginFlow(oauthCfg, opts).Run(ctx)
	if err != nil {
		return err
	}
	if tok.RefreshToken == "" {
		// token_access_type=offline を指定していれば必ず得られるはず。
		// 得られない場合、アクセストークンは4時間ほどで失効してしまう。
		return errors.New("リフレッシュトークンを取得できませんでした。" +
			"アプリの設定を確認して再試行してください")
	}

	return auth.NewFileStore().Save(Type, cfg.Name, tok)
}

// retryPolicy は API 呼び出しの再試行方針です。
//
// Dropbox SDK は既定では再試行しません。一方 Google API のクライアントは
// 内部で再試行を持つため、以前は「Drive だけ再試行が効いて Dropbox は
// 効かない」という非対称がありました。
func retryPolicy() *retry.Policy {
	return &retry.Policy{
		MaxRetries:     5,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		// 429 が返るときは Retry-After が付くが、極端に長い場合に
		// 待ち続けないよう上限を設ける。
		MaxRetryAfter: 5 * time.Minute,
		Retryable409Tags: []string{
			// 同一の名前空間に対する書き込みが多すぎる場合。
			// 同時処理数を上げると起こりやすい。
			"too_many_write_operations",
			"too_many_requests",
		},
	}
}

// newClient は Dropbox API のクライアントを作ります。
func newClient(ctx context.Context, cfg Config) (dbx.ContextClient, error) {
	oauthCfg, err := oauth2Config(cfg)
	if err != nil {
		return nil, err
	}

	store := auth.NewFileStore()
	tok, err := store.Load(Type, cfg.Name)
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			return nil, fmt.Errorf("dropbox %q は未認証です。hbg auth login %s で認証してください", cfg.Name, cfg.Name)
		}
		return nil, err
	}

	// アクセストークンは4時間ほどで失効するが、リフレッシュトークンから
	// 自動更新され、更新結果はディスクへ書き戻される。
	src := auth.PersistingTokenSource(
		oauthCfg.TokenSource(ctx, tok), store, Type, cfg.Name, tok)

	return dbx.NewContext(dbxapi.Config{
		TokenSource: src,
		RetryPolicy: retryPolicy(),
	}), nil
}
