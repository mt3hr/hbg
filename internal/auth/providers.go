package auth

import (
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// DropboxRedirectPorts は Dropbox の認可で使うポートの候補です。
//
// Dropbox はリダイレクト URI がアプリ登録時の値と完全に一致している必要があり、
// Google や Microsoft のように任意のポートを使えません。
// あらかじめ登録しておいた候補を順に試します。
var DropboxRedirectPorts = []int{53682, 53683, 53684}

// DropboxRedirectHost は Dropbox のリダイレクト URI に書くホストです。
//
// Dropbox は localhost と 127.0.0.1 を別物として扱い、登録された文字列と
// 完全に一致するものしか受け付けません。どちらに揃えるかだけの話ですが、
// http のループバックを localhost で通す慣行が広く使われている
// （rclone も同じ理由で localhost を使っています）ので、それに合わせます。
// 待ち受け自体は 127.0.0.1 のままで、URI の見た目だけを合わせます。
//
// 以前はここが既定の 127.0.0.1 のままだった一方、下の登録手順は localhost を
// 案内していました。Dropbox は完全一致でしか照合しないので、手順どおりに
// 登録しても認可画面で invalid redirect uri になっていました。
const DropboxRedirectHost = "localhost"

// DropboxRedirectURIs は、アプリ登録時に Redirect URIs へ入れる文字列です。
// 認可要求に載せる URI と同じ組み立て方をするので、ずれません。
func DropboxRedirectURIs() []string {
	uris := make([]string, 0, len(DropboxRedirectPorts))
	for _, port := range DropboxRedirectPorts {
		uris = append(uris, RedirectURI(DropboxRedirectHost, port))
	}
	return uris
}

// DropboxScopes は hbg が必要とする Dropbox の権限です。
//
// hbg が呼ぶのはファイルの一覧・取得・作成・削除・移動だけなので、
// 共有リンクなどの権限は要求しません。
var DropboxScopes = []string{
	"account_info.read",
	"files.metadata.read",
	"files.metadata.write",
	"files.content.read",
	"files.content.write",
}

// dropboxEndpoint は Dropbox の OAuth2 エンドポイントです。
var dropboxEndpoint = oauth2.Endpoint{
	AuthURL:   "https://www.dropbox.com/oauth2/authorize",
	TokenURL:  "https://api.dropboxapi.com/oauth2/token",
	AuthStyle: oauth2.AuthStyleInParams,
}

// DropboxOAuth2Config は Dropbox 用の oauth2.Config を返します。
func DropboxOAuth2Config(creds ClientCredentials) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret, // PKCE を使う場合は空
		Endpoint:     dropboxEndpoint,
		Scopes:       DropboxScopes,
	}
}

// DropboxAuthCodeOptions は Dropbox の認可要求に付ける追加パラメータです。
//
// token_access_type=offline を指定しないとリフレッシュトークンが得られず、
// アクセストークンが失効するたびに再認証が必要になります。
// 以前はこれを指定しておらず、約4時間ごとに認証をやり直す必要がありました。
func DropboxAuthCodeOptions() []oauth2.AuthCodeOption {
	return []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("token_access_type", "offline"),
	}
}

// GoogleDriveScope は hbg が必要とする Google Drive の権限です。
//
// hbg は利用者が既に持っている任意のファイルを読み書きするため、
// アプリが作成したファイルだけを扱う drive.file では足りません。
const GoogleDriveScope = "https://www.googleapis.com/auth/drive"

// GoogleDriveOAuth2Config は Google Drive 用の oauth2.Config を返します。
func GoogleDriveOAuth2Config(creds ClientCredentials) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{GoogleDriveScope},
	}
}

// GoogleAuthCodeOptions は Google の認可要求に付ける追加パラメータです。
//
// AccessTypeOffline でリフレッシュトークンを要求し、
// ApprovalForce で再認可のときも確実にリフレッシュトークンを得ます。
func GoogleAuthCodeOptions() []oauth2.AuthCodeOption {
	return []oauth2.AuthCodeOption{
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
	}
}

// MicrosoftScopes は hbg が必要とする Microsoft の権限です。
//
// offline_access がないとリフレッシュトークンが得られず、
// 1時間ほどで再認証が必要になります。
var MicrosoftScopes = []string{
	"offline_access",
	"Files.ReadWrite.All",
	"User.Read",
}

// microsoftEndpoint は Microsoft の OAuth2 エンドポイントです。
//
// consumers（個人用）と organizations（職場・学校）の両方を受け付ける
// common を使います。
var microsoftEndpoint = oauth2.Endpoint{
	AuthURL:  "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
	TokenURL: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
}

// MicrosoftOAuth2Config は Microsoft 用の oauth2.Config を返します。
//
// テナントを指定すると、その組織の利用者だけに絞れます。
func MicrosoftOAuth2Config(creds ClientCredentials, tenant string) *oauth2.Config {
	endpoint := microsoftEndpoint
	if tenant != "" {
		endpoint = oauth2.Endpoint{
			AuthURL:  "https://login.microsoftonline.com/" + tenant + "/oauth2/v2.0/authorize",
			TokenURL: "https://login.microsoftonline.com/" + tenant + "/oauth2/v2.0/token",
		}
	}

	return &oauth2.Config{
		ClientID: creds.ClientID,
		// パブリッククライアントなのでシークレットは持ちません。
		Endpoint: endpoint,
		Scopes:   MicrosoftScopes,
	}
}
