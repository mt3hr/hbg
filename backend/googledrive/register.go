package googledrive

import (
	"context"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "Google Drive",
		ConfigDoc: `  - name: googledrive
    type: googledrive
    # client_id: ${HBG_GOOGLE_CLIENT_ID}
    # client_secret: ${HBG_GOOGLE_CLIENT_SECRET}
    # drive_id: 共有ドライブのID（省略時はマイドライブ）
    # native_files: error  # Google ドキュメントの扱い（error または skip）
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			return New(ctx, Config{
				Name:         name,
				ClientID:     params.Get("client_id"),
				ClientSecret: params.Get("client_secret"),
				DriveID:      params.Get("drive_id"),
				RootFolderID: params.Get("root_folder_id"),
				NativeFiles:  params.Get("native_files"),
			})
		},
	})
}
