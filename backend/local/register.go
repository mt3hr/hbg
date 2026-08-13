package local

import (
	"context"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "ローカルファイルシステム",
		ConfigDoc: `  - name: local
    type: local
`,
		New: func(_ context.Context, name string, _ backend.Params) (storage.Storage, error) {
			// ローカルファイルシステムは接続も認証も要らない。
			return New(name), nil
		},
	})
}
