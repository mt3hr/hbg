// Package auth は OAuth2 の認可とトークンの保存を扱います。
//
// 認可コードの受け取りには、ローカルに一時的な HTTP サーバを立てる
// 「ループバックリダイレクト」を使います（RFC 8252）。
//
// 以前は OOB フロー（urn:ietf:wg:oauth:2.0:oob）でブラウザに表示された
// コードを手で貼り付ける方式でしたが、Google は 2023 年 1 月にこの方式を
// 完全に廃止しており、新規の認可が通らなくなっていました。
package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
)

// DefaultTimeout は、ブラウザでの許可操作を待つ既定の時間です。
const DefaultTimeout = 3 * time.Minute

// ErrDenied は、ユーザーがブラウザ上で許可しなかったことを表します。
var ErrDenied = errors.New("認可が拒否されました")

// Flow は 1 回の認可フローの設定です。
type Flow struct {
	// Config は認可に使う OAuth2 の設定です。RedirectURL は Run が設定します。
	Config *oauth2.Config

	// FixedPorts は、リダイレクト先のポートを固定したい場合に指定します。
	//
	// Dropbox はリダイレクト URI がアプリ登録時の値と完全に一致している
	//必要があり、任意のポートを使えません。Google と Microsoft は
	// ループバックなら任意ポートを許可するため、空で構いません。
	// 先頭から順に試し、使えるものを採用します。
	FixedPorts []int

	// UsePKCE を真にすると PKCE（RFC 7636）を使います。
	// クライアントシークレットを持たない公開クライアントでは必須です。
	UsePKCE bool

	// ExtraAuthCodeOptions は認可 URL に付ける追加のパラメータです。
	ExtraAuthCodeOptions []oauth2.AuthCodeOption

	// ExtraExchangeOptions はコード交換時に付ける追加のパラメータです。
	ExtraExchangeOptions []oauth2.AuthCodeOption

	// Timeout は許可操作を待つ時間です。0 なら DefaultTimeout。
	Timeout time.Duration

	// OpenBrowser が偽の場合、ブラウザを自動で開きません。
	OpenBrowser bool

	// Prompt は、ユーザーへの案内を表示する関数です。
	// nil の場合は何も表示しません。
	Prompt func(authURL string)
}

// listen はリダイレクトを受けるためのリスナーを開きます。
func (f *Flow) listen(ctx context.Context) (net.Listener, error) {
	var lc net.ListenConfig

	if len(f.FixedPorts) == 0 {
		ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("ローカルの待ち受けを開始できませんでした: %w", err)
		}
		return ln, nil
	}

	var lastErr error
	for _, port := range f.FixedPorts {
		ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			return ln, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("リダイレクト用のポート %v をどれも使用できませんでした。"+
		"他のアプリケーションが使用している可能性があります: %w", f.FixedPorts, lastErr)
}

// callbackResult は、リダイレクトで受け取った内容です。
type callbackResult struct {
	code string
	err  error
}

// Run は認可フローを実行し、取得したトークンを返します。
func (f *Flow) Run(ctx context.Context) (*oauth2.Token, error) {
	if f.Config == nil {
		return nil, errors.New("OAuth2 の設定がありません")
	}

	ln, err := f.listen(ctx)
	if err != nil {
		return nil, err
	}
	defer ln.Close()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return nil, fmt.Errorf("待ち受けのアドレスを取得できませんでした: %v", ln.Addr())
	}
	f.Config.RedirectURL = fmt.Sprintf("http://127.0.0.1:%d/callback", addr.Port)

	// state は CSRF 対策。要求ごとにランダムでなければならない。
	state, err := randomState()
	if err != nil {
		return nil, err
	}

	authOpts := append([]oauth2.AuthCodeOption{}, f.ExtraAuthCodeOptions...)
	exchangeOpts := append([]oauth2.AuthCodeOption{}, f.ExtraExchangeOptions...)

	if f.UsePKCE {
		verifier := oauth2.GenerateVerifier()
		authOpts = append(authOpts, oauth2.S256ChallengeOption(verifier))
		exchangeOpts = append(exchangeOpts, oauth2.VerifierOption(verifier))
	}

	authURL := f.Config.AuthCodeURL(state, authOpts...)

	results := make(chan callbackResult, 1)
	srv := &http.Server{
		Handler:           f.handler(state, results),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if f.Prompt != nil {
		f.Prompt(authURL)
	}
	if f.OpenBrowser {
		if err := OpenBrowser(authURL); err != nil {
			// ブラウザを開けなくても、URL は表示済みなので続行できる。
			_ = err
		}
	}

	timeout := f.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("認可を %s 待ちましたが完了しませんでした", timeout)
	case res := <-results:
		if res.err != nil {
			return nil, res.err
		}
		tok, err := f.Config.Exchange(ctx, res.code, exchangeOpts...)
		if err != nil {
			return nil, fmt.Errorf("認可コードからトークンを取得できませんでした: %w", err)
		}
		return tok, nil
	}
}

// handler はリダイレクトを受け取る HTTP ハンドラを返します。
func (f *Flow) handler(wantState string, results chan<- callbackResult) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		send := func(res callbackResult) {
			select {
			case results <- res:
			default: // すでに結果を受け取っている
			}
		}

		if errCode := q.Get("error"); errCode != "" {
			desc := q.Get("error_description")
			writeHTML(w, http.StatusBadRequest, "認可されませんでした",
				"ターミナルに戻ってやり直してください。")
			if errCode == "access_denied" {
				send(callbackResult{err: ErrDenied})
				return
			}
			send(callbackResult{err: fmt.Errorf("認可に失敗しました: %s %s", errCode, desc)})
			return
		}

		// state を検証する。一致しない要求は攻撃の可能性があるので受け付けない。
		if got := q.Get("state"); got != wantState {
			writeHTML(w, http.StatusBadRequest, "不正な要求です",
				"この画面を閉じて、もう一度やり直してください。")
			send(callbackResult{err: errors.New("state が一致しません（CSRF の可能性があります）")})
			return
		}

		code := q.Get("code")
		if code == "" {
			writeHTML(w, http.StatusBadRequest, "認可コードが取得できませんでした",
				"ターミナルに戻ってやり直してください。")
			send(callbackResult{err: errors.New("認可コードが空です")})
			return
		}

		writeHTML(w, http.StatusOK, "認証が完了しました",
			"この画面を閉じて、ターミナルに戻ってください。")
		send(callbackResult{code: code})
	})
	return mux
}

// ParseManualRedirect は、手動認可モードでユーザーが貼り付けた
// リダイレクト先 URL から認可コードを取り出します。
//
// OOB フローと違い、貼り付けるのはコードではなく URL 全体です。
// state の検証をローカルで行えるため、CSRF に対する耐性があります。
func ParseManualRedirect(raw, wantState string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("URL を解釈できませんでした: %w", err)
	}
	q := u.Query()

	if errCode := q.Get("error"); errCode != "" {
		if errCode == "access_denied" {
			return "", ErrDenied
		}
		return "", fmt.Errorf("認可に失敗しました: %s %s", errCode, q.Get("error_description"))
	}
	if got := q.Get("state"); got != wantState {
		return "", errors.New("state が一致しません（CSRF の可能性があります）")
	}
	code := q.Get("code")
	if code == "" {
		return "", errors.New("URL に認可コードが含まれていません")
	}
	return code, nil
}

// randomState は CSRF 対策の state を生成します。
func randomState() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("乱数を生成できませんでした: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// writeHTML はブラウザに表示する簡単なページを書きます。
func writeHTML(w http.ResponseWriter, status int, title, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<!doctype html>
<html lang="ja">
<head><meta charset="utf-8"><title>hbg</title></head>
<body style="font-family: sans-serif; max-width: 40em; margin: 4em auto; line-height: 1.7;">
<h1 style="font-size: 1.4em;">%s</h1>
<p>%s</p>
</body>
</html>
`, title, message)
}
