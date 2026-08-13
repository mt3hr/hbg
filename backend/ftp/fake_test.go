package ftp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	ftpserver "github.com/fclairamb/ftpserverlib"
	"github.com/spf13/afero"
)

// 試験用の FTP サーバーです。
//
// docker も外部のサーバーもなしに、実際のやりとりを通して
// 試験できるようにするためのものです。fclairamb/ftpserverlib は
// サーバー側の実装を持っているので、それをこの計算機の
// ディレクトリに向けます。
//
// MLSD も MFMT も本物の手続きが流れます。

const (
	testUser     = "試験利用者"
	testPassword = "ひみつ"
)

// fakeFTP は試験用のサーバーです。
type fakeFTP struct {
	root     string
	fs       afero.Fs
	addr     string
	listener net.Listener

	// setTimeSupported が偽なら、更新時刻の書き換えに応じません。
	// 古いサーバーを模します。
	setTimeSupported bool
}

// GetSettings はサーバーの設定を返します。
func (f *fakeFTP) GetSettings() (*ftpserver.Settings, error) {
	return &ftpserver.Settings{
		// 待ち受け先は呼び出し側で用意したものを使う。
		Listener: f.listener,
		// 機械向けの一覧を使う。日付が秒まで取れる。
		DisableMLSD: false,
		// MFMT に応じない古いサーバーも模せるようにしておく。
		DisableMFMT: !f.setTimeSupported,
	}, nil
}

func (f *fakeFTP) ClientConnected(ftpserver.ClientContext) (string, error) {
	return "試験用のサーバーです", nil
}

func (f *fakeFTP) ClientDisconnected(ftpserver.ClientContext) {}

func (f *fakeFTP) AuthUser(_ ftpserver.ClientContext, user, pass string) (ftpserver.ClientDriver, error) {
	if user != testUser || pass != testPassword {
		return nil, errors.New("ログインできません")
	}
	return f.fs, nil
}

func (f *fakeFTP) GetTLSConfig() (*tls.Config, error) {
	return nil, errors.New("この試験では暗号化を使いません")
}

// startFakeFTP は試験用のサーバーを立ち上げます。
func startFakeFTP(t *testing.T, opts ...func(*fakeFTP)) *fakeFTP {
	t.Helper()

	root := t.TempDir()
	f := &fakeFTP{
		root:             root,
		fs:               afero.NewBasePathFs(afero.NewOsFs(), root),
		setTimeSupported: true,
	}
	for _, o := range opts {
		o(f)
	}

	// 待ち受け先を先に決めておく。あとから繋ぎ先が分かるようにするため。
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("待ち受けを開けません: %v", err)
	}
	f.listener = ln
	f.addr = ln.Addr().String()

	srv := ftpserver.NewFtpServer(f)
	srv.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := srv.Listen(); err != nil {
		_ = ln.Close()
		t.Fatalf("サーバーを用意できません: %v", err)
	}

	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Stop() })

	return f
}

// connect は試験用サーバーへ繋いだストレージを返します。
func (f *fakeFTP) connect(t *testing.T, mutate ...func(*Config)) *Storage {
	t.Helper()

	host, port, found := strings.Cut(f.addr, ":")
	if !found {
		t.Fatalf("待ち受け先を解釈できません: %q", f.addr)
	}

	cfg := Config{
		Name:     "偽ftp",
		Host:     host,
		Port:     mustAtoi(t, port),
		User:     testUser,
		Password: testPassword,
		// 試験用のサーバーは暗号化を用意していない。
		TLS: TLSNone,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("接続できません: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("数として読めません: %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}
