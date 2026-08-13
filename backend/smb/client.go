package smb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudsoda/go-smb2"
	"github.com/mt3hr/hbg/internal/hbghome"
	"github.com/spf13/viper"
)

// defaultPort は SMB の既定のポートです。
const defaultPort = 445

// dialTimeout は接続の待ち時間です。
const dialTimeout = 30 * time.Second

// Config は SMB ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。
	Name string

	// Host は接続先の計算機です。
	//
	// "\\server\share" という書き方は受け付けません。Windows の
	// パスの書き方とファイル名の区切りが混ざって、どこまでが計算機名で
	// どこからが共有名なのか決められなくなるためです。
	// 計算機と共有はそれぞれ host と share で指定してください。
	Host string
	// Port は接続先のポートです。0 なら 445 です。
	Port int
	// Share は共有の名前です。
	Share string

	// User はログイン名です。
	User string
	// Password は合言葉です。
	// 設定ファイルへの直接記述は避け、${環境変数} での指定を推奨します。
	Password string
	// Domain は所属です。省略できます。
	Domain string

	// Root を指定すると、共有の中のその下を起点として扱います。
	Root string

	// fsOverride は試験のためにファイル操作を差し替えるためのものです。
	fsOverride fileSystem
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

// validate は接続を試みる前に設定の不足を知らせます。
func (c Config) validate() error {
	switch {
	case c.Host == "":
		return errors.New("接続先（host）が指定されていません")
	case c.Share == "":
		return errors.New("共有の名前（share）が指定されていません")
	}

	// "\\server\share" と書かれた場合に、何が悪いのかを伝える。
	if len(c.Host) >= 2 && c.Host[0] == '\\' && c.Host[1] == '\\' {
		return fmt.Errorf(`host には計算機の名前だけを指定してください（%q が指定されました）。`+
			`"\\計算機\共有" ではなく host と share に分けて書きます`, c.Host)
	}
	return nil
}

// connect は共有に接続します。
func connect(ctx context.Context, cfg Config) (fileSystem, error) {
	if cfg.fsOverride != nil {
		return cfg.fsOverride, nil
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	user, password, err := resolveCredentials(cfg)
	if err != nil {
		return nil, err
	}

	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.addr())
	if err != nil {
		return nil, fmt.Errorf("%s へ接続できません: %w", cfg.addr(), err)
	}

	dialer := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     user,
			Password: password,
			Domain:   cfg.Domain,
		},
	}

	session, err := dialer.DialConn(ctx, conn, cfg.addr())
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("%s@%s のログインに失敗しました: %w", user, cfg.Host, err)
	}

	share, err := session.Mount(cfg.Share)
	if err != nil {
		_ = session.Logoff()
		return nil, fmt.Errorf("共有 %s に繋げません: %w", cfg.Share, err)
	}

	return smbShare{share: share}, nil
}

// resolveCredentials はログイン情報を決めます。
//
// 設定に書かれていればそれを、なければ
// $HOME/hbg/credentials/smb_<名前>.yaml を読みます。
func resolveCredentials(cfg Config) (user, password string, err error) {
	if cfg.User != "" {
		return cfg.User, cfg.Password, nil
	}

	file, err := credentialsPath(cfg.Name)
	if err != nil {
		return "", "", err
	}
	stored, err := readCredentialsFile(file)
	if err != nil {
		return "", "", err
	}
	if stored == nil {
		// 誰でも読める共有もあるので、名前なしでも試す。
		return "", "", nil
	}
	return stored.User, stored.Password, nil
}

// credentialsPath はログイン情報を置く既定の場所です。
func credentialsPath(name string) (string, error) {
	dir, err := hbghome.CredentialsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "smb_"+name+".yaml"), nil
}

// storedCredentials はログイン情報のファイルの中身です。
type storedCredentials struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Domain   string `mapstructure:"domain"`
}

// readCredentialsFile はログイン情報のファイルを読みます。
// ファイルがなければ nil を返します。
func readCredentialsFile(file string) (*storedCredentials, error) {
	if _, err := os.Stat(file); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	v := viper.New()
	v.SetConfigFile(file)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("ログイン情報 %s を読めません: %w", file, err)
	}

	var stored storedCredentials
	if err := v.Unmarshal(&stored); err != nil {
		return nil, fmt.Errorf("ログイン情報 %s を解釈できません: %w", file, err)
	}
	if stored.User == "" {
		return nil, fmt.Errorf("ログイン情報 %s に user が必要です", file)
	}
	return &stored, nil
}
