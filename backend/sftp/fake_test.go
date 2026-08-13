package sftp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"

	sftpc "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// 試験用の SSH + SFTP サーバーです。
//
// docker も外部のサーバーもなしに、実際のやりとりを通して
// 試験できるようにするためのものです。pkg/sftp はサーバー側の
// 実装も持っているので、それを本物のファイルシステムに向けます。

// testUser と testPassword は試験用のログイン情報です。
const (
	testUser     = "試験利用者"
	testPassword = "ひみつ"
)

// fakeServer は立ち上げた試験用サーバーです。
type fakeServer struct {
	addr     string
	hostKey  ssh.PublicKey
	rootDir  string
	listener net.Listener

	mu       sync.Mutex
	sessions int
}

// startFakeServer は試験用のサーバーを立ち上げます。
// rootDir 配下を、ログイン直後のディレクトリとして見せます。
func startFakeServer(t *testing.T, rootDir string) *fakeServer {
	t.Helper()

	signer := generateHostKey(t)

	cfg := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == testUser && string(pass) == testPassword {
				return nil, nil
			}
			return nil, errors.New("ログインできません")
		},
	}
	cfg.AddHostKey(signer)

	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("待ち受けを開けません: %v", err)
	}

	s := &fakeServer{
		addr:     ln.Addr().String(),
		hostKey:  signer.PublicKey(),
		rootDir:  filepath.ToSlash(rootDir),
		listener: ln,
	}
	t.Cleanup(func() { _ = ln.Close() })

	go s.acceptLoop(cfg)
	return s
}

// generateHostKey は試験用のホスト鍵を作ります。
func generateHostKey(t *testing.T) ssh.Signer {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ホスト鍵を作れません: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("ホスト鍵を読めません: %v", err)
	}
	return signer
}

func (s *fakeServer) acceptLoop(cfg *ssh.ServerConfig) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn, cfg)
	}
}

func (s *fakeServer) handleConn(conn net.Conn, cfg *ssh.ServerConfig) {
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer sshConn.Close()

	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "session だけを受け付けます")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go s.handleSession(channel, requests)
	}
}

func (s *fakeServer) handleSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	for req := range requests {
		if req.Type != "subsystem" || subsystemName(req.Payload) != "sftp" {
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
			continue
		}
		if req.WantReply {
			_ = req.Reply(true, nil)
		}

		s.mu.Lock()
		s.sessions++
		s.mu.Unlock()

		server, err := sftpc.NewServer(channel, sftpc.WithServerWorkingDirectory(s.rootDir))
		if err != nil {
			_ = channel.Close()
			return
		}
		_ = server.Serve()
		_ = server.Close()
		_ = channel.Close()
		return
	}
}

// subsystemName は subsystem 要求の中身を読み取ります。
func subsystemName(payload []byte) string {
	var msg struct{ Name string }
	if err := ssh.Unmarshal(payload, &msg); err != nil {
		return ""
	}
	return msg.Name
}

// connect は試験用サーバーへ繋いだストレージを返します。
func (s *fakeServer) connect(t *testing.T, mutate ...func(*Config)) (*Storage, []string) {
	t.Helper()

	host, port, err := net.SplitHostPort(s.addr)
	if err != nil {
		t.Fatalf("待ち受け先を解釈できません: %v", err)
	}

	notices := []string{}
	cfg := Config{
		Name:                  "偽sftp",
		Host:                  host,
		User:                  testUser,
		Password:              testPassword,
		KnownHostsFile:        filepath.Join(t.TempDir(), "known_hosts"),
		StrictHostKeyChecking: StrictAcceptNew,
		Notify:                func(m string) { notices = append(notices, m) },
	}
	cfg.Port = mustAtoi(t, port)

	for _, f := range mutate {
		f(&cfg)
	}

	st, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("接続できません: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	return st, notices
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
