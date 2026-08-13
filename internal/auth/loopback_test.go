package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// fakeProvider は認可サーバの代わりをする最小の実装です。
//
// 認可エンドポイントは、ブラウザの代わりにこちらからリダイレクト先へ
// アクセスすることで「ユーザーが許可した」状況を再現します。
type fakeProvider struct {
	server *httptest.Server

	// 受け取った認可要求のパラメータ
	gotChallenge string
	gotMethod    string
	// トークン交換で受け取ったパラメータ
	gotVerifier string
	gotCode     string

	// denyAccess を真にすると、許可されなかった場合を再現します。
	denyAccess bool
}

func newFakeProvider(t *testing.T) *fakeProvider {
	t.Helper()
	p := &fakeProvider{}

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		p.gotVerifier = r.Form.Get("code_verifier")
		p.gotCode = r.Form.Get("code")

		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{
			"access_token": "test-access-token",
			"refresh_token": "test-refresh-token",
			"token_type": "Bearer",
			"expires_in": 3600
		}`)
	})

	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)
	return p
}

func (p *fakeProvider) config() *oauth2.Config {
	return &oauth2.Config{
		ClientID: "test-client",
		Endpoint: oauth2.Endpoint{
			AuthURL:  p.server.URL + "/authorize",
			TokenURL: p.server.URL + "/token",
		},
		Scopes: []string{"test.scope"},
	}
}

// visit は、ブラウザがユーザーの許可後にリダイレクト先へアクセスする動きを再現します。
//
// 別の goroutine から呼ばれるため、t.Fatal 系は使えません（テストの
// goroutine からしか呼べない）。失敗は t.Errorf で報告して戻ります。
func (p *fakeProvider) visit(t *testing.T, authURL string) {
	t.Helper()

	u, err := url.Parse(authURL)
	if err != nil {
		t.Errorf("認可URLを解釈できない: %v", err)
		return
	}
	q := u.Query()
	p.gotChallenge = q.Get("code_challenge")
	p.gotMethod = q.Get("code_challenge_method")

	redirect, err := url.Parse(q.Get("redirect_uri"))
	if err != nil {
		t.Errorf("redirect_uri を解釈できない: %v", err)
		return
	}

	rq := redirect.Query()
	if p.denyAccess {
		rq.Set("error", "access_denied")
	} else {
		rq.Set("code", "test-auth-code")
		rq.Set("state", q.Get("state"))
	}
	redirect.RawQuery = rq.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, redirect.String(), nil)
	if err != nil {
		t.Errorf("要求を組み立てられない: %v", err)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("リダイレクト先へアクセスできない: %v", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestFlowRun(t *testing.T) {
	p := newFakeProvider(t)

	flow := &Flow{
		Config:  p.config(),
		UsePKCE: true,
		Timeout: 10 * time.Second,
		Prompt: func(authURL string) {
			// ブラウザの代わりにテストからアクセスする
			go p.visit(t, authURL)
		},
	}

	tok, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if tok.AccessToken != "test-access-token" {
		t.Errorf("AccessToken = %q", tok.AccessToken)
	}
	if tok.RefreshToken != "test-refresh-token" {
		t.Errorf("RefreshToken = %q（リフレッシュトークンが取得できていない）", tok.RefreshToken)
	}
	if p.gotCode != "test-auth-code" {
		t.Errorf("交換に渡された code = %q", p.gotCode)
	}
}

func TestFlowUsesPKCE(t *testing.T) {
	p := newFakeProvider(t)

	flow := &Flow{
		Config:  p.config(),
		UsePKCE: true,
		Timeout: 10 * time.Second,
		Prompt:  func(authURL string) { go p.visit(t, authURL) },
	}
	if _, err := flow.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if p.gotChallenge == "" {
		t.Error("code_challenge が送られていない")
	}
	if p.gotMethod != "S256" {
		t.Errorf("code_challenge_method = %q, want S256", p.gotMethod)
	}
	if p.gotVerifier == "" {
		t.Error("code_verifier が交換時に送られていない")
	}
}

func TestFlowWithoutPKCE(t *testing.T) {
	p := newFakeProvider(t)

	flow := &Flow{
		Config:  p.config(),
		UsePKCE: false,
		Timeout: 10 * time.Second,
		Prompt:  func(authURL string) { go p.visit(t, authURL) },
	}
	if _, err := flow.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if p.gotChallenge != "" {
		t.Errorf("PKCE を使わない設定なのに code_challenge が送られている: %q", p.gotChallenge)
	}
}

func TestFlowRedirectIsLoopback(t *testing.T) {
	p := newFakeProvider(t)

	var redirectURI string
	flow := &Flow{
		Config:  p.config(),
		Timeout: 10 * time.Second,
		Prompt: func(authURL string) {
			u, _ := url.Parse(authURL)
			redirectURI = u.Query().Get("redirect_uri")
			go p.visit(t, authURL)
		},
	}
	if _, err := flow.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 廃止された OOB フローを使っていないこと
	if strings.Contains(redirectURI, "urn:ietf:wg:oauth:2.0:oob") {
		t.Errorf("廃止された OOB フローが使われている: %q", redirectURI)
	}
	if !strings.HasPrefix(redirectURI, "http://127.0.0.1:") {
		t.Errorf("redirect_uri = %q, ループバックであるべき", redirectURI)
	}
}

func TestFlowDenied(t *testing.T) {
	p := newFakeProvider(t)
	p.denyAccess = true

	flow := &Flow{
		Config:  p.config(),
		Timeout: 10 * time.Second,
		Prompt:  func(authURL string) { go p.visit(t, authURL) },
	}

	_, err := flow.Run(context.Background())
	if !errors.Is(err, ErrDenied) {
		t.Errorf("err = %v, want ErrDenied", err)
	}
}

// state が一致しない要求は受け付けないこと。
func TestFlowRejectsBadState(t *testing.T) {
	p := newFakeProvider(t)

	flow := &Flow{
		Config:  p.config(),
		Timeout: 10 * time.Second,
		Prompt: func(authURL string) {
			go func() {
				u, _ := url.Parse(authURL)
				redirect, _ := url.Parse(u.Query().Get("redirect_uri"))
				rq := redirect.Query()
				rq.Set("code", "test-auth-code")
				rq.Set("state", "攻撃者が用意した値")
				redirect.RawQuery = rq.Encode()

				req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, redirect.String(), nil)
				if err != nil {
					return
				}
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}()
		},
	}

	_, err := flow.Run(context.Background())
	if err == nil {
		t.Fatal("state が一致しないのに成功した")
	}
	if !strings.Contains(err.Error(), "state") {
		t.Errorf("err = %v, state の不一致を示すべき", err)
	}
}

func TestFlowTimeout(t *testing.T) {
	p := newFakeProvider(t)

	flow := &Flow{
		Config:  p.config(),
		Timeout: 100 * time.Millisecond,
		Prompt:  func(string) {}, // 誰もアクセスしてこない
	}

	start := time.Now()
	_, err := flow.Run(context.Background())
	if err == nil {
		t.Fatal("待ち時間を過ぎても成功している")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("待ち時間が長すぎる: %v", elapsed)
	}
}

func TestFlowContextCancel(t *testing.T) {
	p := newFakeProvider(t)

	ctx, cancel := context.WithCancel(context.Background())
	flow := &Flow{
		Config:  p.config(),
		Timeout: 10 * time.Second,
		Prompt:  func(string) { cancel() },
	}

	_, err := flow.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestFlowFixedPorts(t *testing.T) {
	p := newFakeProvider(t)

	// 使用中のポートを1つ用意し、次の候補に落ちることを確かめる。
	busy := httptest.NewServer(http.NewServeMux())
	defer busy.Close()
	busyPort := mustPort(t, busy.URL)

	var redirectURI string
	flow := &Flow{
		Config:     p.config(),
		FixedPorts: []int{busyPort, 0}, // 0 は「任意のポート」として使える
		Timeout:    10 * time.Second,
		Prompt: func(authURL string) {
			u, _ := url.Parse(authURL)
			redirectURI = u.Query().Get("redirect_uri")
			go p.visit(t, authURL)
		},
	}

	if _, err := flow.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(redirectURI, ":"+strconv.Itoa(busyPort)+"/") {
		t.Errorf("使用中のポートが選ばれている: %q", redirectURI)
	}
}

func TestParseManualRedirect(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantState string
		wantCode  string
		wantErr   string
	}{
		{
			name:      "正常",
			raw:       "http://127.0.0.1:53682/callback?code=abc&state=xyz",
			wantState: "xyz",
			wantCode:  "abc",
		},
		{
			name:      "state が一致しない",
			raw:       "http://127.0.0.1:53682/callback?code=abc&state=other",
			wantState: "xyz",
			wantErr:   "state",
		},
		{
			name:      "コードがない",
			raw:       "http://127.0.0.1:53682/callback?state=xyz",
			wantState: "xyz",
			wantErr:   "認可コード",
		},
		{
			name:      "拒否された",
			raw:       "http://127.0.0.1:53682/callback?error=access_denied",
			wantState: "xyz",
			wantErr:   "拒否",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, err := ParseManualRedirect(tt.raw, tt.wantState)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("エラーになるはずだった")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("err = %v, %q を含むべき", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseManualRedirect: %v", err)
			}
			if code != tt.wantCode {
				t.Errorf("code = %q, want %q", code, tt.wantCode)
			}
		})
	}
}

func mustPort(t *testing.T, rawURL string) int {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("ポート番号を解釈できない: %v", err)
	}
	return port
}
