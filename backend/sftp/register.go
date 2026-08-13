package sftp

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "SFTP（SSH 越しのファイル転送）",
		ConfigDoc: `  # - name: nas
  #   type: sftp
  #   host: サーバーのホスト名
  #   user: ログイン名
  #   port: 22
  #   key_file: 秘密鍵の場所（省略時は $HOME/hbg/credentials/sftp_<名前>.key）
  #   key_passphrase: ${SFTP_KEY_PASSPHRASE}
  #   password: ${SFTP_PASSWORD}
  #   use_agent: false
  #   known_hosts_file: 省略時は $HOME/hbg/configs/known_hosts
  #   strict_host_key_checking: yes  # yes / accept-new / no
  #   root: 起点にするディレクトリ
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			port, err := intParam(params, "port")
			if err != nil {
				return nil, fmt.Errorf("sftp %s: %w", name, err)
			}

			return New(ctx, Config{
				Name:                  name,
				Host:                  params.Get("host"),
				Port:                  port,
				User:                  params.Get("user"),
				Password:              params.Get("password"),
				KeyFile:               params.Get("key_file"),
				KeyPassphrase:         params.Get("key_passphrase"),
				UseAgent:              params.Get("use_agent") == "true",
				KnownHostsFile:        params.Get("known_hosts_file"),
				StrictHostKeyChecking: params.Get("strict_host_key_checking"),
				Root:                  params.Get("root"),
				Notify: func(message string) {
					fmt.Fprintf(os.Stderr, "hbg: %s\n", message)
				},
			})
		},
	})
}

// intParam は数として指定された設定を読みます。
func intParam(params backend.Params, key string) (int, error) {
	raw := params.Get(key)
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s には数を指定してください（%q が指定されました）", key, raw)
	}
	return n, nil
}
