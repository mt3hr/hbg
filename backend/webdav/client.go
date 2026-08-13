package webdav

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// 相手の種類。
//
// WebDAV には更新時刻を書き換える標準の方法がありません。
// {DAV:}getlastmodified はサーバーが管理する項目で、PROPPATCH では
// 変えられないことになっています。Nextcloud と ownCloud は
// X-OC-Mtime という独自のヘッダを受け付けるので、そこだけ使い分けます。
const (
	// PresetGeneric は一般の WebDAV サーバーです。更新時刻を保持できません。
	PresetGeneric = "generic"
	// PresetNextcloud は Nextcloud です。
	PresetNextcloud = "nextcloud"
	// PresetOwncloud は ownCloud です。
	PresetOwncloud = "owncloud"
)

// mtimeHeader は Nextcloud / ownCloud が受け付ける更新時刻のヘッダです。
const mtimeHeader = "X-OC-Mtime"

// Config は WebDAV ストレージの設定です。
type Config struct {
	// Name は設定ファイルで付けた名前です。
	Name string

	// URL は WebDAV の入口です。
	URL string
	// User はログイン名です。
	User string
	// Password は合言葉です。
	// 設定ファイルへの直接記述は避け、${環境変数} での指定を推奨します。
	// Nextcloud などではアプリ用の合言葉を発行して使ってください。
	Password string

	// Preset は相手の種類です。
	// "generic"（既定）、"nextcloud"、"owncloud" のいずれかです。
	Preset string

	// Root を指定すると、その下を起点として扱います。
	Root string

	// transportOverride は試験のために通信の経路を差し替えるためのものです。
	transportOverride http.RoundTripper
}

func (c Config) preset() string {
	if c.Preset == "" {
		return PresetGeneric
	}
	return c.Preset
}

// canSetModTime は更新時刻を保持できる相手かを返します。
func (c Config) canSetModTime() bool {
	switch c.preset() {
	case PresetNextcloud, PresetOwncloud:
		return true
	}
	return false
}

// validate は接続を試みる前に設定の不足を知らせます。
func (c Config) validate() error {
	if c.URL == "" {
		return errors.New("入口（url）が指定されていません")
	}
	if !strings.HasPrefix(c.URL, "http://") && !strings.HasPrefix(c.URL, "https://") {
		return fmt.Errorf("url は http:// か https:// で始めてください（%q が指定されました）", c.URL)
	}

	switch c.preset() {
	case PresetGeneric, PresetNextcloud, PresetOwncloud:
	default:
		return fmt.Errorf("preset には %q, %q, %q のいずれかを指定してください（%q が指定されました）",
			PresetGeneric, PresetNextcloud, PresetOwncloud, c.Preset)
	}
	return nil
}

// mtimeHeaders は更新時刻を伝えるヘッダを組み立てます。
func mtimeHeaders(t time.Time, enabled bool) map[string]string {
	if !enabled || t.IsZero() {
		return nil
	}
	return map[string]string{mtimeHeader: fmt.Sprint(t.Unix())}
}
