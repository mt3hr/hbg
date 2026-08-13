package onedrive

import (
	"context"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/storage"
)

func init() {
	backend.Register(backend.Descriptor{
		Type:    Type,
		Summary: "OneDrive（Microsoft Graph）",
		ConfigDoc: `  # - name: onedrive
  #   type: onedrive
  #   client_id: ${HBG_MICROSOFT_CLIENT_ID}
  #   drive_type: personal  # personal / business / sharepoint
  #   tenant: 組織の識別子（省略可）
  #   drive_id: ドライブのID（省略可）
  #   site_id: SharePoint のサイト（drive_type が sharepoint のとき）
  #   root: 起点にするディレクトリ
`,
		New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
			return New(ctx, Config{
				Name:      name,
				ClientID:  params.Get("client_id"),
				Tenant:    params.Get("tenant"),
				DriveType: params.Get("drive_type"),
				DriveID:   params.Get("drive_id"),
				SiteID:    params.Get("site_id"),
				Root:      params.Get("root"),
			})
		},
	})
}
