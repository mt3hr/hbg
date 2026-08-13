package ftp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	ftpc "github.com/jlaffaye/ftp"
)

// FTP は1つの接続で1つのやりとりしかできません。制御用の通信路と
// データ用の通信路を対にして使うためです。同時に転送するには
// そのぶん接続が要るので、使い回せるようにまとめておきます。

// dialTimeout は接続の待ち時間です。
const dialTimeout = 30 * time.Second

// connPool は FTP の接続をまとめて持ちます。
type connPool struct {
	cfg Config
	// max は同時に開く接続の上限です。
	max int

	// slots は接続を開く権利です。空きがなければ待ちます。
	slots chan struct{}

	mu     sync.Mutex
	idle   []*ftpc.ServerConn
	closed bool
}

func newConnPool(cfg Config, maxConns int) *connPool {
	if maxConns < 1 {
		maxConns = 1
	}
	return &connPool{
		cfg:   cfg,
		max:   maxConns,
		slots: make(chan struct{}, maxConns),
	}
}

// get は使える接続を1つ取り出します。
// 使い終わったら put か discard を呼んでください。
func (p *connPool) get(ctx context.Context) (*ftpc.ServerConn, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return nil, errors.New("すでに閉じられています")
	}
	if n := len(p.idle); n > 0 {
		conn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()

		// 寝ている間に切れていることがあるので確かめる。
		if err := conn.NoOp(); err == nil {
			return conn, nil
		}
		_ = conn.Quit()

		p.mu.Lock()
	}
	p.mu.Unlock()

	conn, err := dial(ctx, p.cfg)
	if err != nil {
		<-p.slots
		return nil, err
	}
	return conn, nil
}

// put は接続を戻します。
func (p *connPool) put(conn *ftpc.ServerConn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = conn.Quit()
		<-p.slots
		return
	}
	p.idle = append(p.idle, conn)
	p.mu.Unlock()

	<-p.slots
}

// discard は壊れた接続を捨てます。
func (p *connPool) discard(conn *ftpc.ServerConn) {
	_ = conn.Quit()
	<-p.slots
}

// close はすべての接続を閉じます。
func (p *connPool) close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()

	var firstErr error
	for _, conn := range idle {
		if err := conn.Quit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dial は接続してログインします。
func dial(ctx context.Context, cfg Config) (*ftpc.ServerConn, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	options := []ftpc.DialOption{
		ftpc.DialWithContext(ctx),
		ftpc.DialWithTimeout(dialTimeout),
	}

	switch cfg.tls() {
	case TLSExplicit:
		// AUTH TLS で暗号化に切り替える。既定はこちら。
		options = append(options, ftpc.DialWithExplicitTLS(cfg.tlsConfig()))
	case TLSImplicit:
		// はじめから暗号化された通信路で繋ぐ。
		options = append(options, ftpc.DialWithTLS(cfg.tlsConfig()))
	}
	if cfg.DisableEPSV {
		options = append(options, ftpc.DialWithDisabledEPSV(true))
	}
	if cfg.DisableMLSD {
		options = append(options, ftpc.DialWithDisabledMLSD(true))
	}

	conn, err := ftpc.Dial(cfg.addr(), options...)
	if err != nil {
		return nil, fmt.Errorf("%s へ接続できません: %w", cfg.addr(), err)
	}

	// 以前の実装はここでの失敗を確かめておらず、ログインできていない
	// ままあとの操作に進んでいた。何が起きたのか分からない失敗になる。
	if err := conn.Login(cfg.user(), cfg.password()); err != nil {
		_ = conn.Quit()
		return nil, fmt.Errorf("%s@%s のログインに失敗しました: %w", cfg.user(), cfg.Host, err)
	}
	return conn, nil
}

// tlsConfig は暗号化の設定を組み立てます。
func (c Config) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName: c.Host,
		//nolint:gosec // 利用者が明示的に選んだ場合にだけ真になる
		InsecureSkipVerify: c.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
}

func (c Config) addr() string {
	return net.JoinHostPort(c.Host, fmt.Sprint(c.port()))
}
