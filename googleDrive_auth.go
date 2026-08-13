package hbg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mt3hr/hbg/internal/auth"
	"golang.org/x/oauth2"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// GoogleDriveStorageType は Google Drive ストレージの種別名です。
const GoogleDriveStorageType = "googledrive"

// googleDriveOAuth2Config は設定から oauth2.Config を組み立てます。
func googleDriveOAuth2Config(cfg GoogleDriveConfig) (*oauth2.Config, error) {
	creds, err := auth.ResolveGoogle(cfg.ClientID, cfg.ClientSecret)
	if err != nil {
		return nil, err
	}
	return auth.GoogleDriveOAuth2Config(creds), nil
}

// GoogleDriveLogin は対話的に認可を行い、トークンを保存します。
// hbg auth login から呼ばれます。
func GoogleDriveLogin(ctx context.Context, cfg GoogleDriveConfig, opts AuthLoginOptions) error {
	oauthCfg, err := googleDriveOAuth2Config(cfg)
	if err != nil {
		return err
	}

	flow := &auth.Flow{
		Config:               oauthCfg,
		UsePKCE:              true,
		ExtraAuthCodeOptions: auth.GoogleAuthCodeOptions(),
		OpenBrowser:          opts.OpenBrowser,
		Prompt:               opts.Prompt,
	}

	tok, err := flow.Run(ctx)
	if err != nil {
		return err
	}
	if tok.RefreshToken == "" {
		// リフレッシュトークンが得られないと、アクセストークンの失効後に
		// また手動で認可し直すことになる。
		return errors.New("リフレッシュトークンを取得できませんでした。" +
			"Google Cloud の OAuth 同意画面で、いちど hbg のアクセス権を取り消してから再試行してください")
	}

	return auth.NewFileStore().Save(GoogleDriveStorageType, cfg.Name, tok)
}

// newGoogleDriveService は保存済みのトークンを使って Drive のクライアントを作ります。
func newGoogleDriveService(cfg GoogleDriveConfig) (*drive.Service, error) {
	oauthCfg, err := googleDriveOAuth2Config(cfg)
	if err != nil {
		return nil, err
	}

	store := auth.NewFileStore()
	tok, err := store.Load(GoogleDriveStorageType, cfg.Name)
	if err != nil {
		if errors.Is(err, auth.ErrNoToken) {
			return nil, fmt.Errorf("googledrive %q は未認証です。hbg auth login %s で認証してください", cfg.Name, cfg.Name)
		}
		return nil, err
	}

	ctx := context.Background()
	// 期限切れのアクセストークンは自動更新され、更新結果はディスクへ書き戻される。
	// 以前は更新結果がメモリ上にしか残らず、毎回の起動で古い状態から始まっていた。
	src := auth.PersistingTokenSource(
		oauthCfg.TokenSource(ctx, tok), store, GoogleDriveStorageType, cfg.Name, tok)

	client := oauth2.NewClient(ctx, wrapGoogleTokenSource(src, cfg.Name))
	return drive.NewService(ctx, option.WithHTTPClient(client))
}

// wrapGoogleTokenSource は、トークン更新の失敗に説明を添える TokenSource を返します。
type googleTokenSource struct {
	src  oauth2.TokenSource
	name string
}

func wrapGoogleTokenSource(src oauth2.TokenSource, name string) oauth2.TokenSource {
	return &googleTokenSource{src: src, name: name}
}

func (g *googleTokenSource) Token() (*oauth2.Token, error) {
	tok, err := g.src.Token()
	if err == nil {
		return tok, nil
	}

	// OAuth 同意画面が「テスト」のままだと、リフレッシュトークンが
	// 7日で失効する。実運用でもっとも踏みやすい落とし穴なので明示する。
	if strings.Contains(err.Error(), "invalid_grant") {
		return nil, fmt.Errorf("googledrive %q のトークンが無効になりました: %w\n"+
			"  Google Cloud の OAuth 同意画面が「テスト」のままだと、\n"+
			"  リフレッシュトークンは7日で失効します。公開ステータスを\n"+
			"  「本番環境」にしたうえで hbg auth login %s をやり直してください",
			g.name, err, g.name)
	}
	return nil, err
}
