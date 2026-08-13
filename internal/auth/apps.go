package auth

import (
	"fmt"
	"os"
	"strings"
)

// OAuth クライアントの識別子。
//
// ビルド時に -ldflags で埋め込めます。
//
//	go build -ldflags "-X github.com/mt3hr/hbg/internal/auth.GoogleClientID=... \
//	                   -X github.com/mt3hr/hbg/internal/auth.GoogleClientSecret=..." ./cmd/hbg
//
// 以前はソースに直接書かれており、リポジトリの履歴に残ってしまっていました。
// RFC 8252 が述べるとおり、デスクトップアプリのクライアントシークレットは
// 本質的に秘密にできません。そのため hbg では次の方針を取ります。
//
//   - Dropbox は PKCE を使うのでシークレット自体が不要です（アプリキーのみ）。
//   - Microsoft も「パブリッククライアント」として登録すればシークレットは不要です。
//   - Google は installed app でもシークレットを要求するため、
//     利用者自身の Google Cloud プロジェクトを使うことを既定とします。
//
// なお、過去に公開されてしまった鍵は失効させる以外に対処がありません。
var (
	// DropboxAppKey は Dropbox のアプリキーです。PKCE を使うのでシークレットは不要です。
	DropboxAppKey = ""

	// GoogleClientID は Google OAuth クライアントのIDです。
	GoogleClientID = ""
	// GoogleClientSecret は Google OAuth クライアントのシークレットです。
	GoogleClientSecret = ""

	// MicrosoftClientID は Microsoft のアプリ（クライアント）IDです。
	// パブリッククライアントとして登録すればシークレットは不要です。
	MicrosoftClientID = ""
)

// 環境変数名。
const (
	EnvDropboxAppKey      = "HBG_DROPBOX_APP_KEY"
	EnvGoogleClientID     = "HBG_GOOGLE_CLIENT_ID"
	EnvGoogleClientSecret = "HBG_GOOGLE_CLIENT_SECRET"
	EnvMicrosoftClientID  = "HBG_MICROSOFT_CLIENT_ID"
)

// ClientCredentials は OAuth クライアントの識別情報です。
type ClientCredentials struct {
	ClientID     string
	ClientSecret string
}

// ResolveDropbox は Dropbox のアプリキーを解決します。
//
// 優先順位は、設定ファイルの値 → 環境変数 → ビルド時に埋め込まれた値です。
func ResolveDropbox(fromConfig string) (ClientCredentials, error) {
	key := firstNonEmpty(fromConfig, os.Getenv(EnvDropboxAppKey), DropboxAppKey)
	if key == "" {
		return ClientCredentials{}, missingCredentialsError("dropbox")
	}
	return ClientCredentials{ClientID: key}, nil
}

// ResolveGoogle は Google の OAuth クライアント情報を解決します。
func ResolveGoogle(idFromConfig, secretFromConfig string) (ClientCredentials, error) {
	id := firstNonEmpty(idFromConfig, os.Getenv(EnvGoogleClientID), GoogleClientID)
	secret := firstNonEmpty(secretFromConfig, os.Getenv(EnvGoogleClientSecret), GoogleClientSecret)
	if id == "" || secret == "" {
		return ClientCredentials{}, missingCredentialsError("googledrive")
	}
	return ClientCredentials{ClientID: id, ClientSecret: secret}, nil
}

// indentLines は各行に接頭辞を付けて連結します。
// 案内の中に URI の一覧を埋め込むために使います。
func indentLines(lines []string, indent string) string {
	var b strings.Builder
	for i, line := range lines {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(indent)
		b.WriteString(line)
	}
	return b.String()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// missingCredentialsError は、OAuth クライアントが未設定であることと
// その設定手順を伝えるエラーを返します。
func missingCredentialsError(storageType string) error {
	return fmt.Errorf("%s の OAuth クライアントが設定されていません。\n\n%s",
		storageType, setupInstructions(storageType))
}

// setupInstructions は、利用者が自分でアプリを登録するための手順を返します。
func setupInstructions(storageType string) string {
	switch storageType {
	case "dropbox":
		return `Dropbox アプリを登録してください:

  1. https://www.dropbox.com/developers/apps でアプリを作成
     - Choose an API: Scoped access
     - Choose the type of access: Full Dropbox（または App folder）
  2. Settings タブの OAuth 2 > Redirect URIs に以下を登録
` + indentLines(DropboxRedirectURIs(), "       ") + `
  3. Permissions タブで以下の権限を有効にして Submit
       files.metadata.read  files.metadata.write
       files.content.read   files.content.write
       account_info.read
  4. Settings タブの App key を設定ファイルに書く

       Dropbox:
         - name: dropbox
           app_key: ${HBG_DROPBOX_APP_KEY}

     または環境変数 HBG_DROPBOX_APP_KEY に設定する。

  App secret は必要ありません（PKCE を使います）。`

	case "googledrive":
		return `Google Cloud プロジェクトを用意してください:

  1. https://console.cloud.google.com/ でプロジェクトを作成
  2. Google Drive API を有効化
  3. OAuth 同意画面を設定し、公開ステータスを「本番環境」にする
     ※「テスト」のままだとリフレッシュトークンが7日で失効します
  4. 認証情報 > OAuth クライアント ID を作成
     - アプリケーションの種類: デスクトップアプリ
  5. 発行されたIDとシークレットを設定ファイルに書く

       GoogleDrive:
         - name: googledrive
           client_id: ${HBG_GOOGLE_CLIENT_ID}
           client_secret: ${HBG_GOOGLE_CLIENT_SECRET}

     または環境変数 HBG_GOOGLE_CLIENT_ID / HBG_GOOGLE_CLIENT_SECRET に設定する。

  Google Drive の全体にアクセスする権限は「制限付きスコープ」に
  分類されており、アプリを一般公開するには年次のセキュリティ評価が
  必要です。そのため hbg では利用者自身のプロジェクトを使います。`
	}
	return ""
}

// ResolveMicrosoft は Microsoft のアプリIDを解決します。
//
// パブリッククライアントとして登録し、PKCE を使うので
// シークレットは不要です。
func ResolveMicrosoft(fromConfig string) (ClientCredentials, error) {
	id := firstNonEmpty(fromConfig, os.Getenv(EnvMicrosoftClientID), MicrosoftClientID)
	if id == "" {
		return ClientCredentials{}, missingCredentialsError("onedrive")
	}
	return ClientCredentials{ClientID: id}, nil
}
