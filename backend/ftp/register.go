package ftp

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
		Summary: "FTP（既定で AUTH TLS）",
		ConfigDoc: `  # - name: ftp
  #   type: ftp
  #   host: サーバーのホスト名
  #   user: ログイン名          # 省略すると anonymous
  #   password: ${FTP_PASSWORD}
  #   port: 21
  #   tls: explicit            # explicit / implicit / none
  #   insecure_skip_verify: false
  #   disable_epsv: false      # 古いサーバー向け
  #   disable_mlsd: false      # 一覧の形が壊れているサーバー向け
  #   max_conns: 4
  #   root: 起点にするディレクトリ
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			port, err := intParam(params, "port")
			if err != nil {
				return nil, fmt.Errorf("ftp %s: %w", name, err)
			}
			maxConns, err := intParam(params, "max_conns")
			if err != nil {
				return nil, fmt.Errorf("ftp %s: %w", name, err)
			}

			s, err := New(ctx, Config{
				Name:               name,
				Host:               params.Get("host"),
				Port:               port,
				User:               params.Get("user"),
				Password:           params.Get("password"),
				TLS:                params.Get("tls"),
				InsecureSkipVerify: params.Get("insecure_skip_verify") == "true",
				DisableEPSV:        params.Get("disable_epsv") == "true",
				DisableMLSD:        params.Get("disable_mlsd") == "true",
				MaxConns:           maxConns,
				Root:               params.Get("root"),
			})
			if err != nil {
				return nil, err
			}

			s.SetNotifier(func(message string) {
				fmt.Fprintf(os.Stderr, "hbg: %s\n", message)
			})
			return s, nil
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
