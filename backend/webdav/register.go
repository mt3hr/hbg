package webdav

import (
	"context"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "WebDAV（Nextcloud / ownCloud など）",
		ConfigDoc: `  # - name: webdav
  #   type: webdav
  #   url: https://例.invalid/remote.php/dav/files/利用者名/
  #   user: ログイン名
  #   password: ${WEBDAV_PASSWORD}
  #   preset: generic  # generic / nextcloud / owncloud
  #   root: 起点にするディレクトリ
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			return New(ctx, Config{
				Name:     name,
				URL:      params.Get("url"),
				User:     params.Get("user"),
				Password: params.Get("password"),
				Preset:   params.Get("preset"),
				Root:     params.Get("root"),
			})
		},
	})
}
