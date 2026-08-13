package auth

import (
	"strings"
	"testing"
)

func TestResolveDropbox(t *testing.T) {
	t.Run("設定の値が最優先", func(t *testing.T) {
		t.Setenv(EnvDropboxAppKey, "from-env")
		creds, err := ResolveDropbox("from-config")
		if err != nil {
			t.Fatalf("ResolveDropbox: %v", err)
		}
		if creds.ClientID != "from-config" {
			t.Errorf("ClientID = %q, want from-config", creds.ClientID)
		}
	})

	t.Run("設定になければ環境変数", func(t *testing.T) {
		t.Setenv(EnvDropboxAppKey, "from-env")
		creds, err := ResolveDropbox("")
		if err != nil {
			t.Fatalf("ResolveDropbox: %v", err)
		}
		if creds.ClientID != "from-env" {
			t.Errorf("ClientID = %q, want from-env", creds.ClientID)
		}
	})

	t.Run("空白だけの値は指定なしとして扱う", func(t *testing.T) {
		t.Setenv(EnvDropboxAppKey, "from-env")
		creds, err := ResolveDropbox("   ")
		if err != nil {
			t.Fatalf("ResolveDropbox: %v", err)
		}
		if creds.ClientID != "from-env" {
			t.Errorf("ClientID = %q, want from-env", creds.ClientID)
		}
	})

	t.Run("どこにもなければ手順を案内する", func(t *testing.T) {
		t.Setenv(EnvDropboxAppKey, "")
		_, err := ResolveDropbox("")
		if err == nil {
			t.Fatal("エラーになるはずだった")
		}
		for _, want := range []string{"dropbox", "Redirect URIs", "files.content.read", "App secret は必要ありません"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("案内に %q が含まれていない", want)
			}
		}
	})

	// PKCE を使うのでシークレットは不要。
	t.Run("シークレットは要求しない", func(t *testing.T) {
		t.Setenv(EnvDropboxAppKey, "key-only")
		creds, err := ResolveDropbox("")
		if err != nil {
			t.Fatalf("ResolveDropbox: %v", err)
		}
		if creds.ClientSecret != "" {
			t.Errorf("ClientSecret = %q, 空であるべき", creds.ClientSecret)
		}
	})
}

func TestResolveGoogle(t *testing.T) {
	t.Run("IDとシークレットが揃えば解決する", func(t *testing.T) {
		creds, err := ResolveGoogle("id", "secret")
		if err != nil {
			t.Fatalf("ResolveGoogle: %v", err)
		}
		if creds.ClientID != "id" || creds.ClientSecret != "secret" {
			t.Errorf("creds = %+v", creds)
		}
	})

	t.Run("片方だけでは足りない", func(t *testing.T) {
		t.Setenv(EnvGoogleClientID, "")
		t.Setenv(EnvGoogleClientSecret, "")
		if _, err := ResolveGoogle("id", ""); err == nil {
			t.Error("シークレットがないのに成功した")
		}
		if _, err := ResolveGoogle("", "secret"); err == nil {
			t.Error("IDがないのに成功した")
		}
	})

	t.Run("同意画面の落とし穴を案内する", func(t *testing.T) {
		t.Setenv(EnvGoogleClientID, "")
		t.Setenv(EnvGoogleClientSecret, "")
		_, err := ResolveGoogle("", "")
		if err == nil {
			t.Fatal("エラーになるはずだった")
		}
		if !strings.Contains(err.Error(), "7日で失効") {
			t.Error("テスト状態でトークンが失効する件が案内されていない")
		}
	})
}

// ソースに認証情報が直接埋め込まれていないことを確認します。
// 以前は Dropbox のアプリキーとシークレット、Google の
// クライアントシークレットがソースに書かれ、履歴に残っていました。
func TestNoHardcodedCredentials(t *testing.T) {
	if DropboxAppKey != "" {
		t.Errorf("DropboxAppKey がソースに埋め込まれている: %q", DropboxAppKey)
	}
	if GoogleClientID != "" {
		t.Errorf("GoogleClientID がソースに埋め込まれている: %q", GoogleClientID)
	}
	if GoogleClientSecret != "" {
		t.Errorf("GoogleClientSecret がソースに埋め込まれている: %q", GoogleClientSecret)
	}
}

func TestDropboxAuthCodeOptionsRequestsOfflineAccess(t *testing.T) {
	cfg := DropboxOAuth2Config(ClientCredentials{ClientID: "key"})
	url := cfg.AuthCodeURL("state", DropboxAuthCodeOptions()...)

	// これがないとリフレッシュトークンが得られず、
	// アクセストークンが4時間ほどで失効するたびに再認証が必要になる。
	if !strings.Contains(url, "token_access_type=offline") {
		t.Errorf("token_access_type=offline が付いていない: %s", url)
	}
	for _, scope := range DropboxScopes {
		if !strings.Contains(url, strings.ReplaceAll(scope, ".", ".")) {
			t.Errorf("scope %q が要求されていない", scope)
		}
	}
}

func TestGoogleAuthCodeOptionsRequestsOfflineAccess(t *testing.T) {
	cfg := GoogleDriveOAuth2Config(ClientCredentials{ClientID: "id", ClientSecret: "secret"})
	url := cfg.AuthCodeURL("state", GoogleAuthCodeOptions()...)

	if !strings.Contains(url, "access_type=offline") {
		t.Errorf("access_type=offline が付いていない: %s", url)
	}
	if !strings.Contains(url, "approval_prompt=force") && !strings.Contains(url, "prompt=consent") {
		t.Errorf("再認可でリフレッシュトークンを確実に得る指定がない: %s", url)
	}
}
