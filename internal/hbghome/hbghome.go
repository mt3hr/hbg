// Package hbghome は hbg が使うファイルの置き場所を解決します。
//
// 設定・認証情報・ログ・キャッシュはすべて 1 つのディレクトリの下にまとめます。
// 既定は $HOME/hbg で、環境変数 HBG_HOME で変更できます。
//
//	$HOME/hbg/
//	├── configs/
//	│   ├── config.yaml      設定ファイル
//	│   └── known_hosts      SFTP のホスト鍵（将来用）
//	├── tokens/              OAuth トークン
//	├── credentials/         ストレージ固有の資格情報
//	├── logs/                ログ
//	├── caches/              キャッシュ・再開情報
//	└── shell_history        対話シェルの履歴
//
// hbg が読み書きするのはこの配置だけです。
package hbghome

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// EnvHome は根ディレクトリを上書きする環境変数名です。
const EnvHome = "HBG_HOME"

// パーミッション。認証情報を含むため、所有者だけが読めるようにします。
const (
	// DirPerm はディレクトリのパーミッションです。
	DirPerm fs.FileMode = 0o700
	// FilePerm は設定・トークンなど秘密を含むファイルのパーミッションです。
	FilePerm fs.FileMode = 0o600
)

// Root は hbg の根ディレクトリを返します。
// HBG_HOME が設定されていればそれを、なければ $HOME/hbg を返します。
// ディレクトリの作成は行いません。
func Root() (string, error) {
	if v := os.Getenv(EnvHome); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", fmt.Errorf("%s の解決に失敗しました: %w", EnvHome, err)
		}
		return abs, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("ホームディレクトリを特定できませんでした: %w", err)
	}
	return filepath.Join(home, "hbg"), nil
}

// 根ディレクトリ直下のディレクトリ名。
const (
	configsDirName     = "configs"
	tokensDirName      = "tokens"
	credentialsDirName = "credentials"
	logsDirName        = "logs"
	cachesDirName      = "caches"
)

// ConfigFileName は設定ファイルの名前です。
const ConfigFileName = "config.yaml"

func sub(name string) (string, error) {
	root, err := Root()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, name), nil
}

// ConfigsDir は設定ファイルを置くディレクトリを返します。
func ConfigsDir() (string, error) { return sub(configsDirName) }

// TokensDir は OAuth トークンを置くディレクトリを返します。
func TokensDir() (string, error) { return sub(tokensDirName) }

// CredentialsDir はストレージ固有の資格情報を置くディレクトリを返します。
func CredentialsDir() (string, error) { return sub(credentialsDirName) }

// LogsDir はログを置くディレクトリを返します。
func LogsDir() (string, error) { return sub(logsDirName) }

// CachesDir はキャッシュや再開情報を置くディレクトリを返します。
func CachesDir() (string, error) { return sub(cachesDirName) }

// ConfigFile は設定ファイルのパスを返します。
func ConfigFile() (string, error) {
	dir, err := ConfigsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, ConfigFileName), nil
}

// KnownHostsFile は SFTP のホスト鍵を保存するパスを返します。
//
// ~/.ssh/known_hosts とは分けています。hbg が他のツールの設定を
// 書き換えないようにするためです。
func KnownHostsFile() (string, error) {
	dir, err := ConfigsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "known_hosts"), nil
}

// ShellHistoryFile は対話シェルの履歴ファイルのパスを返します。
func ShellHistoryFile() (string, error) {
	root, err := Root()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "shell_history"), nil
}

// TokenFile は指定したストレージ種別・名前のトークンファイルのパスを返します。
func TokenFile(storageType, name string) (string, error) {
	dir, err := TokensDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("%s_%s.json", storageType, name)), nil
}

// EnsureDir はディレクトリを（親ごと）作成します。すでにあれば何もしません。
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, DirPerm); err != nil {
		return fmt.Errorf("ディレクトリを作成できませんでした %s: %w", dir, err)
	}
	return nil
}

// EnsureParentDir は、指定したファイルの親ディレクトリを作成します。
func EnsureParentDir(file string) error {
	return EnsureDir(filepath.Dir(file))
}

// WriteSecretFile は秘密を含むファイルを所有者のみ読み書き可能な権限で書きます。
// 親ディレクトリがなければ作成します。
func WriteSecretFile(path string, data []byte) error {
	if err := EnsureParentDir(path); err != nil {
		return err
	}
	// 既存ファイルのパーミッションは os.WriteFile では変更されないため、
	// 明示的に開き直して権限を指定する。
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, FilePerm)
	if err != nil {
		return fmt.Errorf("ファイルを開けませんでした %s: %w", path, err)
	}
	defer f.Close()

	if err := f.Chmod(FilePerm); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Windows では Chmod がほぼ無効。失敗しても書き込み自体は続行する。
		_ = err
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("ファイルを書き込めませんでした %s: %w", path, err)
	}
	return f.Close()
}
