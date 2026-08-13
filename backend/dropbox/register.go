// Package dropbox は Dropbox を storage.Storage として登録します。
//
// 実装そのものは当面リポジトリのルートにある旧実装を使い、
// legacy のアダプタ越しに新しいインターフェースへ適合させます。
// 作り直しが済んだらこのパッケージが実装本体になります。
package dropbox

import (
	"context"
	"time"

	"github.com/mt3hr/hbg"
	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/backend/legacy"
	"github.com/mt3hr/hbg/storage"
)

// Type はこのバックエンドの種別名です。
const Type = "dropbox"

// features は Dropbox にできることです。
func features() *storage.Features {
	return &storage.Features{
		// Dropbox は更新時刻を UTC の秒に丸める。
		ModTimePrecision: time.Second,
		// 書き込み時にのみ指定できる。あとから変更する手段はない。
		CanSetModTime:   true,
		CaseInsensitive: true,
		// content hash は取得できるが、旧実装がメタデータから捨てている。
		// 作り直しの際に有効にする。
		Hashes:       nil,
		ImplicitDirs: true,
		EmptyDirs:    true,
		// アップロードは完了時にはじめて見えるので実質不可分。
		AtomicPut:   true,
		MaxFileSize: 350 * 1024 * 1024 * 1024,
	}
}

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "Dropbox",
		ConfigDoc: `Dropbox:
  - name: dropbox
    # app_key: ${HBG_DROPBOX_APP_KEY}
`,
		New: func(_ context.Context, name string, params backend.Params) (storage.Storage, error) {
			inner, err := hbg.NewDropbox(hbg.DropboxConfig{
				Name:        name,
				AppKey:      params.Get("app_key"),
				AccessToken: params.Get("access_token"),
			})
			if err != nil {
				return nil, err
			}
			return legacy.Wrap(inner, features()), nil
		},
	})
}
