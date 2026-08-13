// Package googledrive は Google Drive を storage.Storage として登録します。
//
// 実装そのものは当面リポジトリのルートにある旧実装を使い、
// legacy のアダプタ越しに新しいインターフェースへ適合させます。
package googledrive

import (
	"context"
	"time"

	"github.com/mt3hr/hbg"
	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/backend/legacy"
	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "googledrive"

// features は Google Drive にできることです。
func features() *storage.Features {
	return &storage.Features{
		// RFC3339 で表現され、実質ミリ秒まで。
		ModTimePrecision: time.Millisecond,
		CanSetModTime:    true,
		// 同じディレクトリに同名のファイルを作れてしまう点に注意。
		CaseInsensitive: false,
		// md5 などを取得できるが、旧実装は要求していない。
		// 作り直しの際に有効にする。
		Hashes:       nil,
		ImplicitDirs: true,
		EmptyDirs:    true,
		AtomicPut:    true,
	}
}

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "Google Drive",
		ConfigDoc: `GoogleDrive:
  - name: googledrive
    # client_id: ${HBG_GOOGLE_CLIENT_ID}
    # client_secret: ${HBG_GOOGLE_CLIENT_SECRET}
`,
		New: func(_ context.Context, name string, params backend.Params) (storage.Storage, error) {
			inner, err := hbg.NewGoogleDrive(hbg.GoogleDriveConfig{
				Name:         name,
				ClientID:     params.Get("client_id"),
				ClientSecret: params.Get("client_secret"),
			})
			if err != nil {
				return nil, err
			}
			return legacy.Wrap(inner, features()), nil
		},
	})
}
