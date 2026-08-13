package memory

import (
	"context"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "メモリ上のストレージ（テスト用）",
		New: func(_ context.Context, name string, _ backend.Params) (storage.Storage, error) {
			return New(name), nil
		},
	})
}
