package dropbox

import (
	"context"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "Dropbox",
		ConfigDoc: `  - name: dropbox
    type: dropbox
    # app_key: ${HBG_DROPBOX_APP_KEY}
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			return New(ctx, Config{
				Name:        name,
				AppKey:      params.Get("app_key"),
				AccessToken: params.Get("access_token"),
			})
		},
	})
}
