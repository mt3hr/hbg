package hbg

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

// DropboxStorageType は Dropbox ストレージの種別名です。
const DropboxStorageType = "dropbox"

// dropboxOAuth2Config は設定から oauth2.Config を組み立てます。
//
// PKCE を使うためクライアントシークレットは不要です。
func dropboxOAuth2Config(cfg DropboxConfig) (*oauth2.Config, error) {
	creds, err := auth.ResolveDropbox(cfg.AppKey)
	if err != nil {
		return nil, err
	}
	return auth.DropboxOAuth2Config(creds), nil
}

// DropboxLogin は対話的に認可を行い、トークンを保存します。
// hbg auth login から呼ばれます。
func DropboxLogin(ctx context.Context, cfg DropboxConfig, opts AuthLoginOptions) error {
	oauthCfg, err := dropboxOAuth2Config(cfg)
	if err != nil {
		return err
	}

	flow := &auth.Flow{
		Config:  oauthCfg,
		UsePKCE: true,
		// Dropbox はリダイレクトURIの完全一致を要求するため、
		// アプリ登録時に設定したポートを使う。
		FixedPorts:           auth.DropboxRedirectPorts,
		ExtraAuthCodeOptions: auth.DropboxAuthCodeOptions(),
		OpenBrowser:          opts.OpenBrowser,
		Prompt:               opts.Prompt,
	}

	tok, err := flow.Run(ctx)
	if err != nil {
		return err
	}
	if tok.RefreshToken == "" {
		// token_access_type=offline を指定していれば必ず得られるはず。
		// 得られない場合、アクセストークンは4時間ほどで失効してしまう。
		return errors.New("リフレッシュトークンを取得できませんでした。" +
			"アプリの設定を確認して再試行してください")
	}

	return auth.NewFileStore().Save(DropboxStorageType, cfg.Name, tok)
}

// dropboxRetryPolicy は API 呼び出しの再試行方針です。
//
// Dropbox SDK は既定では再試行しません。一方 Google API のクライアントは
// 内部で再試行を持つため、以前は「Drive だけ再試行が効いて Dropbox は
// 効かない」という非対称がありました。
func dropboxRetryPolicy() *retry.Policy {
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

// newDropboxClient は Dropbox API のクライアントを作ります。
func newDropboxClient(cfg DropboxConfig) (dbx.Client, error) {
	// 長期トークンが明示されている場合はそれを使い、認可は行わない。
	if cfg.AccessToken != "" {
		return dbx.New(dbxapi.Config{
			Token:       cfg.AccessToken,
			RetryPolicy: dropboxRetryPolicy(),
		}), nil
	}

	oauthCfg, err := dropboxOAuth2Config(cfg)
	if err != nil {
		return nil, err
	}

	store := auth.NewFileStore()
	tok, err := store.Load(DropboxStorageType, cfg.Name)
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			return nil, fmt.Errorf("dropbox %q は未認証です。hbg auth login %s で認証してください", cfg.Name, cfg.Name)
		}
		return nil, err
	}

	if tok.RefreshToken == "" {
		return nil, fmt.Errorf("dropbox %q のトークンは更新できない古い形式です。"+
			"hbg auth login %s で認証をやり直してください", cfg.Name, cfg.Name)
	}

	ctx := context.Background()
	// アクセストークンは4時間ほどで失効するが、リフレッシュトークンから
	// 自動更新され、更新結果はディスクへ書き戻される。
	src := auth.PersistingTokenSource(
		oauthCfg.TokenSource(ctx, tok), store, DropboxStorageType, cfg.Name, tok)

	return dbx.New(dbxapi.Config{
		TokenSource: src,
		RetryPolicy: dropboxRetryPolicy(),
	}), nil
}
