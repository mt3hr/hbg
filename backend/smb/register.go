package smb

import (
	"context"
	"fmt"
	"strconv"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "SMB（Windows のファイル共有・Samba）",
		ConfigDoc: `  # - name: nas
  #   type: smb
  #   host: 計算機の名前    # "\\計算機\共有" ではなく分けて書く
  #   share: 共有の名前
  #   user: ログイン名
  #   password: ${SMB_PASSWORD}
  #   port: 445
  #   domain: 所属（省略可）
  #   root: 起点にするディレクトリ
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			port, err := intParam(params, "port")
			if err != nil {
				return nil, fmt.Errorf("smb %s: %w", name, err)
			}

			return New(ctx, Config{
				Name:     name,
				Host:     params.Get("host"),
				Port:     port,
				Share:    params.Get("share"),
				User:     params.Get("user"),
				Password: params.Get("password"),
				Domain:   params.Get("domain"),
				Root:     params.Get("root"),
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
