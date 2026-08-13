package sftp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mt3hr/hbg/internal/hbghome"
	"github.com/skeema/knownhosts"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// ホスト鍵の確かめ方。
const (
	// StrictYes は known_hosts にある鍵とだけ接続します。
	StrictYes = "yes"
	// StrictAcceptNew は未知のホストの鍵を記録してから接続します。
	// すでに記録がある場合、鍵が変わっていれば拒否します。
	StrictAcceptNew = "accept-new"
	// StrictNo はホスト鍵を確かめません。
	StrictNo = "no"
)

// defaultPort は SSH の既定のポートです。
const defaultPort = 22

// dialTimeout は接続の待ち時間です。
const dialTimeout = 30 * time.Second

// Config は SFTP ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。
	Name string

	// Host は接続先です。
	Host string
	// Port は接続先のポートです。0 なら 22 です。
	Port int
	// User はログイン名です。
	User string

	// Password でログインする場合に指定します。
	// 設定ファイルへの直接記述は避け、${環境変数} での指定を推奨します。
	Password string
	// KeyFile は秘密鍵の場所です。
	// 省略した場合は $HOME/hbg/credentials/sftp_<名前>.key を探します。
	KeyFile string
	// KeyPassphrase は秘密鍵の複合に使う言葉です。
	KeyPassphrase string
	// UseAgent が真なら ssh-agent の鍵も使います。
	UseAgent bool

	// KnownHostsFile はホスト鍵の記録の場所です。
	// 省略した場合は $HOME/hbg/configs/known_hosts です。
	//
	// ~/.ssh/known_hosts を既定にしていないのは、hbg が他の道具の
	// 設定を書き換えないようにするためです。使いたい場合は明示してください。
	KnownHostsFile string
	// StrictHostKeyChecking はホスト鍵の確かめ方です。
	// "yes"（既定）、"accept-new"、"no" のいずれかです。
	StrictHostKeyChecking string

	// Root を指定すると、その下を起点として扱います。
	Root string

	// Notify は利用者への通知です。ホスト鍵を記録したときなどに呼ばれます。
	// nil なら何もしません。
	Notify func(message string)
}

func (c Config) port() int {
	if c.Port == 0 {
		return defaultPort
	}
	return c.Port
}

func (c Config) addr() string {
	return net.JoinHostPort(c.Host, fmt.Sprint(c.port()))
}

func (c Config) strictness() string {
	if c.StrictHostKeyChecking == "" {
		return StrictYes
	}
	return c.StrictHostKeyChecking
}

func (c Config) notify(format string, a ...any) {
	if c.Notify != nil {
		c.Notify(fmt.Sprintf(format, a...))
	}
}

// validate は接続を試みる前に設定の不足を知らせます。
func (c Config) validate() error {
	switch {
	case c.Host == "":
		return errors.New("接続先（host）が指定されていません")
	case c.User == "":
		return errors.New("ログイン名（user）が指定されていません")
	}

	switch c.strictness() {
	case StrictYes, StrictAcceptNew, StrictNo:
	default:
		return fmt.Errorf("strict_host_key_checking には %q, %q, %q のいずれかを指定してください（%q が指定されました）",
			StrictYes, StrictAcceptNew, StrictNo, c.StrictHostKeyChecking)
	}
	return nil
}

// dial は SSH で接続します。
func dial(ctx context.Context, cfg Config) (*ssh.Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	auths, err := authMethods(ctx, cfg)
	if err != nil {
		return nil, err
	}

	callback, algos, err := hostKeyPolicy(cfg)
	if err != nil {
		return nil, err
	}

	sshCfg := &ssh.ClientConfig{
		User:              cfg.User,
		Auth:              auths,
		HostKeyCallback:   callback,
		HostKeyAlgorithms: algos,
		Timeout:           dialTimeout,
	}

	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.addr())
	if err != nil {
		return nil, fmt.Errorf("%s へ接続できません: %w", cfg.addr(), err)
	}

	c, chans, reqs, err := ssh.NewClientConn(conn, cfg.addr(), sshCfg)
	if err != nil {
		conn.Close()
		return nil, sshError(cfg, err)
	}
	return ssh.NewClient(c, chans, reqs), nil
}

// sshError は接続の失敗に説明を添えます。
func sshError(cfg Config, err error) error {
	path, _ := knownHostsPath(cfg)

	switch {
	case knownhosts.IsHostKeyChanged(err):
		// 中間者攻撃かもしれないし、単にサーバーを入れ替えただけかもしれない。
		// どちらか分からないので、利用者に確かめてもらう。
		return fmt.Errorf("%s のホスト鍵が記録と違います: %w\n"+
			"  サーバーを入れ替えた覚えがなければ、接続先を確かめてください。\n"+
			"  入れ替えた場合は %s から古い行を削除してください",
			cfg.Host, err, path)
	case knownhosts.IsHostUnknown(err):
		return fmt.Errorf("%s のホスト鍵が記録にありません: %w\n"+
			"  はじめて接続する場合は strict_host_key_checking: accept-new を\n"+
			"  指定すると、鍵を確かめたうえで %s に記録します",
			cfg.Host, err, path)
	case strings.Contains(err.Error(), "unable to authenticate"):
		return fmt.Errorf("%s@%s のログインに失敗しました: %w\n"+
			"  鍵の場所やログイン名を確かめてください", cfg.User, cfg.Host, err)
	}
	return fmt.Errorf("%s へ接続できません: %w", cfg.addr(), err)
}

// --- 認証 ---

// authMethods は試すログイン方法を組み立てます。
func authMethods(ctx context.Context, cfg Config) ([]ssh.AuthMethod, error) {
	methods := []ssh.AuthMethod{}

	signer, err := loadKey(cfg)
	if err != nil {
		return nil, err
	}
	if signer != nil {
		methods = append(methods, ssh.PublicKeys(signer))
	}

	if cfg.UseAgent {
		if a := agentSigners(ctx); a != nil {
			methods = append(methods, a)
		}
	}

	if cfg.Password != "" {
		methods = append(methods, ssh.Password(cfg.Password))
	}

	if len(methods) == 0 {
		keyPath, _ := defaultKeyPath(cfg.Name)
		return nil, fmt.Errorf("sftp %q のログイン方法がありません。"+
			"key_file か password を指定するか、%s に秘密鍵を置いてください",
			cfg.Name, keyPath)
	}
	return methods, nil
}

// loadKey は秘密鍵を読みます。鍵がない場合は nil を返します。
func loadKey(cfg Config) (ssh.Signer, error) {
	path := cfg.KeyFile
	explicit := path != ""
	if !explicit {
		var err error
		if path, err = defaultKeyPath(cfg.Name); err != nil {
			return nil, err
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if !explicit && errors.Is(err, os.ErrNotExist) {
			// 既定の場所に置いていないだけ。他の方法を試す。
			return nil, nil
		}
		return nil, fmt.Errorf("秘密鍵 %s を読めません: %w", path, err)
	}

	if cfg.KeyPassphrase != "" {
		signer, keyErr := ssh.ParsePrivateKeyWithPassphrase(data, []byte(cfg.KeyPassphrase))
		if keyErr != nil {
			return nil, fmt.Errorf("秘密鍵 %s を復号できません: %w", path, keyErr)
		}
		return signer, nil
	}

	signer, err := ssh.ParsePrivateKey(data)
	if err != nil {
		var passErr *ssh.PassphraseMissingError
		if errors.As(err, &passErr) {
			return nil, fmt.Errorf("秘密鍵 %s には合言葉が必要です。"+
				"key_passphrase を指定してください", path)
		}
		return nil, fmt.Errorf("秘密鍵 %s を読めません: %w", path, err)
	}
	return signer, nil
}

// defaultKeyPath は秘密鍵の既定の場所です。
func defaultKeyPath(name string) (string, error) {
	dir, err := hbghome.CredentialsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "sftp_"+name+".key"), nil
}

// agentSigners は ssh-agent の鍵を使うログイン方法を返します。
// agent に繋げない場合は nil を返します。
//
// Windows の OpenSSH agent は名前付きパイプなので、ここでは繋がりません。
func agentSigners(ctx context.Context) ssh.AuthMethod {
	sock := os.Getenv("SSH_AUTH_SOCK")
	if sock == "" {
		return nil
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", sock)
	if err != nil {
		return nil
	}
	return ssh.PublicKeysCallback(agent.NewClient(conn).Signers)
}

// --- ホスト鍵 ---

// knownHostsPath はホスト鍵の記録の場所です。
func knownHostsPath(cfg Config) (string, error) {
	if cfg.KnownHostsFile != "" {
		return cfg.KnownHostsFile, nil
	}
	dir, err := hbghome.ConfigsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "known_hosts"), nil
}

// hostKeyPolicy はホスト鍵の確かめ方を組み立てます。
//
// 使えるホスト鍵の種類も一緒に返します。これを指定しないと、
// 記録にあるのとは別の種類の鍵をサーバーが提示することがあり、
// 鍵が変わっていないのに「違う」と判定されてしまいます。
func hostKeyPolicy(cfg Config) (ssh.HostKeyCallback, []string, error) {
	if cfg.strictness() == StrictNo {
		cfg.notify("警告: %s のホスト鍵を確かめずに接続します", cfg.Host)
		//nolint:gosec // 利用者が明示的に選んだ場合にだけ通る
		return ssh.InsecureIgnoreHostKey(), nil, nil
	}

	path, err := knownHostsPath(cfg)
	if err != nil {
		return nil, nil, err
	}
	if err = ensureFile(path); err != nil {
		return nil, nil, err
	}

	db, err := knownhosts.NewDB(path)
	if err != nil {
		return nil, nil, fmt.Errorf("ホスト鍵の記録 %s を読めません: %w", path, err)
	}

	inner := db.HostKeyCallback()
	algos := db.HostKeyAlgorithms(cfg.addr())

	if cfg.strictness() == StrictYes {
		return inner, algos, nil
	}

	// accept-new: 未知のホストだけ記録して先へ進む。
	// 鍵が変わっている場合は記録を書き換えない。
	callback := func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		err := inner(hostname, remote, key)
		if err == nil || !knownhosts.IsHostUnknown(err) {
			return err
		}

		if addErr := appendKnownHost(path, hostname, remote, key); addErr != nil {
			return addErr
		}
		cfg.notify("%s のホスト鍵を %s に記録しました（%s %s）",
			hostname, path, key.Type(), ssh.FingerprintSHA256(key))
		return nil
	}
	return callback, algos, nil
}

// ensureFile はファイルがなければ空で作ります。
func ensureFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("ホスト鍵の記録 %s を作れません: %w", path, err)
	}
	return f.Close()
}

// appendKnownHost はホスト鍵を記録に書き足します。
func appendKnownHost(path, hostname string, remote net.Addr, key ssh.PublicKey) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("ホスト鍵の記録 %s を開けません: %w", path, err)
	}
	defer f.Close()

	if err := knownhosts.WriteKnownHost(f, hostname, remote, key); err != nil {
		return fmt.Errorf("ホスト鍵を %s に書けません: %w", path, err)
	}
	return nil
}
